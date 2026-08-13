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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestTagLabelResolver {

  private static final String METALAKE = "m";
  private static final String CATALOG = "c";
  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("s"), "t");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.LongType.get()),
          Types.NestedField.optional(7, "ssn", Types.StringType.get()));

  @Test
  void resolvesInheritanceChildWinsAndColumnFieldIds() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    when(dispatcher.listTagsInfoForMetadataObject(eq(METALAKE), any(MetadataObject.class)))
        .thenAnswer(
            invocation -> {
              MetadataObject object = invocation.getArgument(1);
              switch (object.type()) {
                case CATALOG:
                  return new Tag[] {tag("data_classification", "internal")};
                case SCHEMA:
                  // more specific than catalog -> must win
                  return new Tag[] {tag("data_classification", "restricted")};
                case TABLE:
                  return new Tag[] {tag("owner_team", "data-platform")};
                case COLUMN:
                  return "ssn".equals(object.name())
                      ? new Tag[] {tag("pii", "ssn")}
                      : new Tag[0];
                default:
                  return new Tag[0];
              }
            });

    IcebergLabels labels =
        new TagLabelResolver(dispatcher, METALAKE, ":").resolveTableLabels(CATALOG, TABLE, SCHEMA);

    // object-labels: schema's data_classification overrides the catalog's; table tag added
    Assertions.assertEquals("restricted", labels.objectLabels().get("data_classification"));
    Assertions.assertEquals("data-platform", labels.objectLabels().get("owner_team"));
    Assertions.assertEquals(2, labels.objectLabels().size());

    // field-labels: only the ssn column (field-id 7); the untagged id column is omitted
    Map<Integer, Map<String, String>> fields = labels.fieldLabels();
    Assertions.assertEquals(1, fields.size());
    Assertions.assertEquals("ssn", fields.get(7).get("pii"));
  }

  @Test
  void joinsMultipleAssignmentValues() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    when(dispatcher.listTagsInfoForMetadataObject(eq(METALAKE), any(MetadataObject.class)))
        .thenAnswer(
            invocation -> {
              MetadataObject object = invocation.getArgument(1);
              return object.type() == MetadataObject.Type.TABLE
                  ? new Tag[] {tag("regulatory_scope", "GDPR", "CCPA")}
                  : new Tag[0];
            });

    IcebergLabels labels =
        new TagLabelResolver(dispatcher, METALAKE, ":").resolveTableLabels(CATALOG, TABLE, SCHEMA);

    Assertions.assertEquals("GDPR,CCPA", labels.objectLabels().get("regulatory_scope"));
  }

  @Test
  void noTagsYieldsEmptyLabels() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    when(dispatcher.listTagsInfoForMetadataObject(eq(METALAKE), any(MetadataObject.class)))
        .thenReturn(new Tag[0]);

    IcebergLabels labels =
        new TagLabelResolver(dispatcher, METALAKE, ":").resolveTableLabels(CATALOG, TABLE, SCHEMA);

    Assertions.assertTrue(labels.isEmpty());
  }

  @Test
  void missingMetadataObjectIsTreatedAsNoLabels() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    when(dispatcher.listTagsInfoForMetadataObject(eq(METALAKE), any(MetadataObject.class)))
        .thenThrow(new NoSuchMetadataObjectException("not a Gravitino-managed object"));

    IcebergLabels labels =
        new TagLabelResolver(dispatcher, METALAKE, ":").resolveTableLabels(CATALOG, TABLE, SCHEMA);

    Assertions.assertTrue(labels.isEmpty());
  }

  @Test
  void serializesToIrcWireShape() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    when(dispatcher.listTagsInfoForMetadataObject(eq(METALAKE), any(MetadataObject.class)))
        .thenAnswer(
            invocation -> {
              MetadataObject object = invocation.getArgument(1);
              if (object.type() == MetadataObject.Type.TABLE) {
                return new Tag[] {tag("data_classification", "restricted")};
              }
              return object.type() == MetadataObject.Type.COLUMN && "ssn".equals(object.name())
                  ? new Tag[] {tag("pii", "ssn")}
                  : new Tag[0];
            });

    IcebergLabels labels =
        new TagLabelResolver(dispatcher, METALAKE, ":").resolveTableLabels(CATALOG, TABLE, SCHEMA);
    ObjectNode json = labels.toJson(new ObjectMapper());

    Assertions.assertEquals(
        "restricted", json.get("object-labels").get("data_classification").asText());
    Assertions.assertEquals(7, json.get("fields").get(0).get("field-id").asInt());
    Assertions.assertEquals("ssn", json.get("fields").get(0).get("labels").get("pii").asText());
  }

  private static Tag tag(String name, String... values) {
    Tag tag = mock(Tag.class);
    when(tag.name()).thenReturn(name);
    when(tag.assignment())
        .thenReturn(values.length == 0 ? Optional.empty() : Optional.of(TagAssignment.ofValues(values)));
    return tag;
  }
}
