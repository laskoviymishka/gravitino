/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.labels;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * A {@link ForeignLabelSource} that reads labels from a remote Iceberg REST catalog over HTTP.
 *
 * <p>It issues raw IRC {@code GET} requests and parses the {@code labels} field directly from the
 * load response, deliberately bypassing the high-level Iceberg REST client — which returns a {@code
 * Table} that does not surface {@code labels}. The table schema is parsed from the same response so
 * field-level labels can be mapped to columns by {@code field-id}.
 *
 * <p><b>Integration seam / POC:</b> this is a minimal HTTP reader. It supports an optional bearer
 * token but not the full IRC auth/config surface, single-level path encoding is best-effort, and it
 * has not been exercised against a live server in this branch. It is the point where a production
 * version would reuse Gravitino's federated-catalog connection config.
 */
public class RestForeignLabelSource implements ForeignLabelSource {

  private final HttpClient httpClient = HttpClient.newHttpClient();
  private final ObjectMapper mapper = new ObjectMapper();
  private final String baseUri;
  private final String prefix;
  private final Namespace namespace;
  private final String bearerToken;

  /**
   * Creates a REST label source.
   *
   * @param baseUri the IRC base URI, e.g. {@code http://host:9001/iceberg}
   * @param prefix the catalog prefix in the IRC path, or {@code null}/empty if none
   * @param namespace the namespace to import tables from
   * @param bearerToken an optional bearer token for {@code Authorization}, or {@code null}
   */
  public RestForeignLabelSource(
      String baseUri, String prefix, Namespace namespace, String bearerToken) {
    this.baseUri = baseUri;
    this.prefix = prefix;
    this.namespace = namespace;
    this.bearerToken = bearerToken;
  }

  @Override
  public List<TableIdentifier> listTables() {
    JsonNode root = get(tablesPath());
    List<TableIdentifier> tables = new ArrayList<>();
    JsonNode identifiers = root.get("identifiers");
    if (identifiers != null && identifiers.isArray()) {
      for (JsonNode identifier : identifiers) {
        List<String> levels = new ArrayList<>();
        identifier.get("namespace").forEach(level -> levels.add(level.asText()));
        tables.add(
            TableIdentifier.of(
                Namespace.of(levels.toArray(new String[0])), identifier.get("name").asText()));
      }
    }
    return tables;
  }

  @Override
  public LoadedTable loadTable(TableIdentifier identifier) {
    JsonNode root = get(tablePath(identifier));
    Schema schema = parseCurrentSchema(root.get("metadata"));
    IcebergLabels labels = IcebergLabels.fromJson(root.get("labels"));
    return new LoadedTable(schema, labels);
  }

  private static Schema parseCurrentSchema(JsonNode metadata) {
    if (metadata == null) {
      return null;
    }
    JsonNode schemas = metadata.get("schemas");
    if (schemas != null && schemas.isArray() && schemas.size() > 0) {
      int currentSchemaId =
          metadata.has("current-schema-id") ? metadata.get("current-schema-id").asInt() : -1;
      JsonNode chosen = schemas.get(0);
      if (currentSchemaId >= 0) {
        for (JsonNode schema : schemas) {
          if (schema.has("schema-id") && schema.get("schema-id").asInt() == currentSchemaId) {
            chosen = schema;
            break;
          }
        }
      }
      return SchemaParser.fromJson(chosen);
    }
    JsonNode legacySchema = metadata.get("schema");
    return legacySchema == null ? null : SchemaParser.fromJson(legacySchema);
  }

  private JsonNode get(String path) {
    try {
      HttpRequest.Builder request =
          HttpRequest.newBuilder(URI.create(baseUri + path))
              .GET()
              .header("Accept", "application/json");
      if (bearerToken != null && !bearerToken.isEmpty()) {
        request.header("Authorization", "Bearer " + bearerToken);
      }
      HttpResponse<String> response =
          httpClient.send(request.build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() / 100 != 2) {
        throw new IllegalStateException(
            "IRC GET " + path + " returned status " + response.statusCode());
      }
      return mapper.readTree(response.body());
    } catch (IOException e) {
      throw new IllegalStateException("Failed IRC GET " + path, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted IRC GET " + path, e);
    }
  }

  private String tablesPath() {
    String prefixSegment = (prefix == null || prefix.isEmpty()) ? "" : encode(prefix) + "/";
    return "/v1/" + prefixSegment + "namespaces/" + encode(namespaceSegment()) + "/tables";
  }

  private String tablePath(TableIdentifier identifier) {
    return tablesPath() + "/" + encode(identifier.name());
  }

  private String namespaceSegment() {
    return String.join("", namespace.levels());
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
