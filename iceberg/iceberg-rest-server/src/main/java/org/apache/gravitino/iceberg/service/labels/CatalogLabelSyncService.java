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

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives label import for one external catalog: enumerates the foreign catalog's tables, reads each
 * table's labels, and materializes them as Gravitino tags on the corresponding federated objects.
 *
 * <p>This is the "trigger" body. It is idempotent (re-import converges — see {@link
 * LabelTagImporter}), so it is safe to invoke on demand or on a schedule (cron). Gravitino has no
 * built-in federation refresh loop, so an operator (a REST op, CLI, or scheduler) invokes {@link
 * #sync()}; wiring those entry points is a thin layer on top of this service.
 */
public class CatalogLabelSyncService {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogLabelSyncService.class);

  private final ForeignLabelSource source;
  private final LabelTagImporter importer;
  private final String targetCatalog;
  private final String sourceName;

  /**
   * Creates a sync service bound to a single external catalog.
   *
   * @param source the foreign label source (transport to the external catalog)
   * @param importer the importer that materializes labels as Gravitino tags
   * @param targetCatalog the Gravitino catalog the federated tables live in
   * @param sourceName the foreign catalog name, recorded on imported tags
   */
  public CatalogLabelSyncService(
      ForeignLabelSource source,
      LabelTagImporter importer,
      String targetCatalog,
      String sourceName) {
    this.source = source;
    this.importer = importer;
    this.targetCatalog = targetCatalog;
    this.sourceName = sourceName;
  }

  /**
   * Imports labels for every table the source exposes. A failure on one table is logged and does
   * not abort the run.
   *
   * @return a summary of the run
   */
  public SyncSummary sync() {
    SyncSummary summary = new SyncSummary();
    for (TableIdentifier identifier : source.listTables()) {
      try {
        ForeignLabelSource.LoadedTable loaded = source.loadTable(identifier);
        LabelTagImporter.ImportResult result =
            importer.importTableLabels(
                targetCatalog, identifier, loaded.schema(), loaded.labels(), sourceName);
        summary.record(identifier, result);
      } catch (Exception e) {
        LOG.warn("Label sync failed for table {}", identifier, e);
        summary.recordFailure(identifier, e);
      }
    }
    LOG.info(
        "Label sync from {} into catalog {}: {} tables, {} imported, {} skipped, {} failed",
        sourceName,
        targetCatalog,
        summary.tablesProcessed(),
        summary.imported().size(),
        summary.skipped().size(),
        summary.failures().size());
    return summary;
  }

  /** Aggregated outcome of a {@link #sync()} run. */
  public static final class SyncSummary {
    private int tablesProcessed;
    private final List<String> imported = new ArrayList<>();
    private final List<String> skipped = new ArrayList<>();
    private final List<String> failures = new ArrayList<>();

    private void record(TableIdentifier identifier, LabelTagImporter.ImportResult result) {
      tablesProcessed++;
      result.imported().forEach(entry -> imported.add(identifier + ": " + entry));
      result.skipped().forEach(entry -> skipped.add(identifier + ": " + entry));
    }

    private void recordFailure(TableIdentifier identifier, Exception e) {
      tablesProcessed++;
      failures.add(identifier + ": " + e.getMessage());
    }

    /**
     * @return the number of tables processed.
     */
    public int tablesProcessed() {
      return tablesProcessed;
    }

    /**
     * @return the labels imported, each prefixed by its table.
     */
    public List<String> imported() {
      return imported;
    }

    /**
     * @return the labels skipped, each prefixed by its table.
     */
    public List<String> skipped() {
      return skipped;
    }

    /**
     * @return the tables whose sync failed, with the failure message.
     */
    public List<String> failures() {
      return failures;
    }
  }
}
