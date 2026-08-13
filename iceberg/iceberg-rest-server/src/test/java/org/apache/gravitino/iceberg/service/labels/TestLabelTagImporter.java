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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.tag.TagValueConstraint;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestLabelTagImporter {

  private static final String METALAKE = "m";
  private static final String CATALOG = "c";
  private static final String SOURCE = "external-catalog";
  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("s"), "t");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.optional(7, "ssn", Types.StringType.get()));

  @Test
  void assignsGovernedTagWhenValueAllowed() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    when(dispatcher.getTag(eq(METALAKE), eq("data_classification")))
        .thenReturn(governed("internal", "restricted"));

    LabelTagImporter.ImportResult result =
        importer(dispatcher)
            .importTableLabels(
                CATALOG, TABLE, SCHEMA, objectLabels("data_classification", "restricted"), SOURCE);

    Assertions.assertEquals(Collections.singletonList("data_classification=restricted"), result.imported());
    verify(dispatcher, never()).createTag(any(), any(), any(), any());
  }

  @Test
  void createsUngovernedTagWhenAbsent() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    when(dispatcher.getTag(eq(METALAKE), eq("domain")))
        .thenThrow(new NoSuchTagException("no tag domain"));

    LabelTagImporter.ImportResult result =
        importer(dispatcher)
            .importTableLabels(CATALOG, TABLE, SCHEMA, objectLabels("domain", "customer"), SOURCE);

    Assertions.assertEquals(Collections.singletonList("domain=customer"), result.imported());
    verify(dispatcher).createTag(eq(METALAKE), eq("domain"), any(), any());
  }

  @Test
  void skipsValueNotAllowedByGovernedTagFailOpen() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    when(dispatcher.getTag(eq(METALAKE), eq("pii"))).thenReturn(governed("email"));

    LabelTagImporter.ImportResult result =
        importer(dispatcher)
            .importTableLabels(CATALOG, TABLE, SCHEMA, objectLabels("pii", "ssn"), SOURCE);

    Assertions.assertTrue(result.imported().isEmpty());
    Assertions.assertEquals(1, result.skipped().size());
    verify(dispatcher, never())
        .associateTagValuesForMetadataObject(any(), any(), any(), any());
  }

  @Test
  void skipsAlreadyAssociatedIdempotently() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    when(dispatcher.getTag(eq(METALAKE), eq("pii"))).thenReturn(governed("ssn"));
    when(dispatcher.associateTagValuesForMetadataObject(any(), any(), any(), any()))
        .thenThrow(new TagAlreadyAssociatedException("already associated"));

    LabelTagImporter.ImportResult result =
        importer(dispatcher)
            .importTableLabels(CATALOG, TABLE, SCHEMA, objectLabels("pii", "ssn"), SOURCE);

    Assertions.assertTrue(result.imported().isEmpty());
    Assertions.assertEquals(1, result.skipped().size());
  }

  @Test
  void mapsColumnLabelByFieldId() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    when(dispatcher.getTag(eq(METALAKE), eq("pii"))).thenReturn(governed("ssn"));

    LabelTagImporter.ImportResult result =
        importer(dispatcher)
            .importTableLabels(CATALOG, TABLE, SCHEMA, fieldLabels(7, "pii", "ssn"), SOURCE);

    Assertions.assertEquals(Collections.singletonList("pii=ssn"), result.imported());
    // associated against a COLUMN object
    verify(dispatcher)
        .associateTagValuesForMetadataObject(
            eq(METALAKE),
            org.mockito.ArgumentMatchers.argThat(o -> o.type() == MetadataObject.Type.COLUMN),
            any(),
            any());
  }

  @Test
  void skipsLabelForUnknownFieldId() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);

    LabelTagImporter.ImportResult result =
        importer(dispatcher)
            .importTableLabels(CATALOG, TABLE, SCHEMA, fieldLabels(99, "x", "y"), SOURCE);

    Assertions.assertTrue(result.imported().isEmpty());
    Assertions.assertEquals(1, result.skipped().size());
    verify(dispatcher, never())
        .associateTagValuesForMetadataObject(any(), any(), any(), any());
  }

  private static LabelTagImporter importer(TagDispatcher dispatcher) {
    return new LabelTagImporter(dispatcher, METALAKE, ":");
  }

  private static Tag governed(String... allowedValues) {
    Tag tag = mock(Tag.class);
    when(tag.valueConstraint()).thenReturn(TagValueConstraint.ofAllowedValues(allowedValues));
    return tag;
  }

  private static IcebergLabels objectLabels(String key, String value) {
    Map<String, String> objectLabels = new LinkedHashMap<>();
    objectLabels.put(key, value);
    return new IcebergLabels(objectLabels, Collections.emptyMap());
  }

  private static IcebergLabels fieldLabels(int fieldId, String key, String value) {
    Map<String, String> labels = new LinkedHashMap<>();
    labels.put(key, value);
    Map<Integer, Map<String, String>> fields = new LinkedHashMap<>();
    fields.put(fieldId, labels);
    return new IcebergLabels(Collections.emptyMap(), fields);
  }
}
