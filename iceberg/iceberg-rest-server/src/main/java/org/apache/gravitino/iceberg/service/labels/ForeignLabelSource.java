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

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * A source of IRC {@code labels} from a foreign catalog, used by {@link CatalogLabelSyncService} to
 * drive tag import. Abstracting the source keeps the sync orchestration testable and lets the
 * concrete transport (see {@link RestForeignLabelSource}) evolve independently.
 */
public interface ForeignLabelSource {

  /**
   * Lists the tables to import labels for.
   *
   * @return the foreign table identifiers
   */
  List<TableIdentifier> listTables();

  /**
   * Loads a table's schema and labels from the foreign catalog.
   *
   * @param identifier the table identifier
   * @return the loaded schema (for mapping labels to columns by field-id) and labels
   */
  LoadedTable loadTable(TableIdentifier identifier);

  /** A table's schema and labels as read from the foreign catalog's load response. */
  final class LoadedTable {
    private final Schema schema;
    private final IcebergLabels labels;

    /**
     * Creates a loaded table.
     *
     * @param schema the table schema
     * @param labels the labels returned by the foreign catalog
     */
    public LoadedTable(Schema schema, IcebergLabels labels) {
      this.schema = schema;
      this.labels = labels;
    }

    /**
     * @return the table schema.
     */
    public Schema schema() {
      return schema;
    }

    /**
     * @return the labels.
     */
    public IcebergLabels labels() {
      return labels;
    }
  }
}
