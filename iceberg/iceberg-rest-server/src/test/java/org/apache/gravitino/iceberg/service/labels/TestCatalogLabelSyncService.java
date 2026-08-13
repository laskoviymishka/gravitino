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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCatalogLabelSyncService {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
  private static final TableIdentifier T1 = TableIdentifier.of(Namespace.of("s"), "t1");
  private static final TableIdentifier T2 = TableIdentifier.of(Namespace.of("s"), "t2");

  @Test
  void importsLabelsForEveryTable() {
    ForeignLabelSource source = mock(ForeignLabelSource.class);
    when(source.listTables()).thenReturn(Arrays.asList(T1, T2));
    when(source.loadTable(T1))
        .thenReturn(new ForeignLabelSource.LoadedTable(SCHEMA, objectLabels("domain", "customer")));
    when(source.loadTable(T2))
        .thenReturn(
            new ForeignLabelSource.LoadedTable(SCHEMA, objectLabels("owner_team", "growth")));

    CatalogLabelSyncService.SyncSummary summary = service(source).sync();

    Assertions.assertEquals(2, summary.tablesProcessed());
    Assertions.assertEquals(2, summary.imported().size());
    Assertions.assertTrue(summary.failures().isEmpty());
  }

  @Test
  void oneTableFailureDoesNotAbortRun() {
    ForeignLabelSource source = mock(ForeignLabelSource.class);
    when(source.listTables()).thenReturn(Arrays.asList(T1, T2));
    when(source.loadTable(T1))
        .thenReturn(new ForeignLabelSource.LoadedTable(SCHEMA, objectLabels("domain", "customer")));
    when(source.loadTable(T2)).thenThrow(new IllegalStateException("boom"));

    CatalogLabelSyncService.SyncSummary summary = service(source).sync();

    Assertions.assertEquals(2, summary.tablesProcessed());
    Assertions.assertEquals(1, summary.imported().size());
    Assertions.assertEquals(1, summary.failures().size());
  }

  private static CatalogLabelSyncService service(ForeignLabelSource source) {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    // Every key is unknown -> importer creates an ungoverned tag and associates it.
    when(dispatcher.getTag(any(), any())).thenThrow(new NoSuchTagException("no tag"));
    LabelTagImporter importer = new LabelTagImporter(dispatcher, "m", ":");
    return new CatalogLabelSyncService(source, importer, "c", "external-catalog");
  }

  private static IcebergLabels objectLabels(String key, String value) {
    Map<String, String> objectLabels = new LinkedHashMap<>();
    objectLabels.put(key, value);
    return new IcebergLabels(objectLabels, Collections.emptyMap());
  }
}
