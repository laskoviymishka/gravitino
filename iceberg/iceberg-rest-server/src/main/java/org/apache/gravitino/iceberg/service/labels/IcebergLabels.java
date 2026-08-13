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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Catalog-provided metadata enrichment returned with a table or view, matching the IRC {@code
 * labels} field proposed in <a href="https://github.com/apache/iceberg/pull/15750">apache/iceberg
 * #15750</a>.
 *
 * <p>This is a proof-of-concept, Gravitino-side representation used to self-serialize the field
 * into load responses, because released iceberg-core does not yet carry a {@code labels} field.
 * The wire shape is {@code object-labels} (a flat key/value map for the object as a whole) plus
 * {@code fields} (per-field labels, each identified by its {@code field-id}).
 */
public class IcebergLabels {

  private final Map<String, String> objectLabels;
  private final Map<Integer, Map<String, String>> fieldLabels;

  /**
   * Creates an {@link IcebergLabels}.
   *
   * @param objectLabels object-level labels (table/view as a whole); may be empty
   * @param fieldLabels per-field labels keyed by Iceberg {@code field-id}; may be empty
   */
  public IcebergLabels(
      Map<String, String> objectLabels, Map<Integer, Map<String, String>> fieldLabels) {
    this.objectLabels = objectLabels == null ? Collections.emptyMap() : objectLabels;
    this.fieldLabels = fieldLabels == null ? Collections.emptyMap() : fieldLabels;
  }

  /**
   * @return object-level labels.
   */
  public Map<String, String> objectLabels() {
    return objectLabels;
  }

  /**
   * @return per-field labels keyed by Iceberg {@code field-id}.
   */
  public Map<Integer, Map<String, String>> fieldLabels() {
    return fieldLabels;
  }

  /**
   * @return {@code true} when there are neither object-level nor field-level labels to emit.
   */
  public boolean isEmpty() {
    return objectLabels.isEmpty() && fieldLabels.isEmpty();
  }

  /**
   * Serializes these labels to the {@code labels} JSON object of the IRC load response.
   *
   * @param mapper the object mapper used to create nodes
   * @return the {@code labels} node ({@code object-labels} and/or {@code fields})
   */
  public ObjectNode toJson(ObjectMapper mapper) {
    ObjectNode root = mapper.createObjectNode();
    if (!objectLabels.isEmpty()) {
      ObjectNode objectNode = root.putObject("object-labels");
      objectLabels.forEach(objectNode::put);
    }
    if (!fieldLabels.isEmpty()) {
      ArrayNode fields = root.putArray("fields");
      fieldLabels.forEach(
          (fieldId, labels) -> {
            if (labels == null || labels.isEmpty()) {
              return;
            }
            ObjectNode field = fields.addObject();
            field.put("field-id", fieldId);
            ObjectNode labelsNode = field.putObject("labels");
            labels.forEach(labelsNode::put);
          });
    }
    return root;
  }

  /**
   * Parses a {@code labels} JSON node (as returned in an IRC load response) back into an {@link
   * IcebergLabels}. Used by the consumer path to extract labels from a foreign catalog's raw
   * {@code loadTable} response, since the high-level REST client does not surface the field.
   *
   * @param node the {@code labels} node (may be {@code null} or missing)
   * @return the parsed labels ({@link #isEmpty() empty} if the node is absent or has neither part)
   */
  public static IcebergLabels fromJson(JsonNode node) {
    Map<String, String> objectLabels = newLabelMap();
    Map<Integer, Map<String, String>> fieldLabels = new LinkedHashMap<>();
    if (node != null && !node.isNull()) {
      JsonNode objectLabelsNode = node.get("object-labels");
      if (objectLabelsNode != null && objectLabelsNode.isObject()) {
        objectLabelsNode
            .fields()
            .forEachRemaining(entry -> objectLabels.put(entry.getKey(), entry.getValue().asText()));
      }
      JsonNode fieldsNode = node.get("fields");
      if (fieldsNode != null && fieldsNode.isArray()) {
        for (JsonNode field : fieldsNode) {
          JsonNode fieldId = field.get("field-id");
          JsonNode labelsNode = field.get("labels");
          if (fieldId != null && labelsNode != null && labelsNode.isObject()) {
            Map<String, String> labels = newLabelMap();
            labelsNode
                .fields()
                .forEachRemaining(entry -> labels.put(entry.getKey(), entry.getValue().asText()));
            fieldLabels.put(fieldId.asInt(), labels);
          }
        }
      }
    }
    return new IcebergLabels(objectLabels, fieldLabels);
  }

  /**
   * @return a mutable, insertion-ordered map suitable for accumulating labels.
   */
  public static Map<String, String> newLabelMap() {
    return new LinkedHashMap<>();
  }
}
