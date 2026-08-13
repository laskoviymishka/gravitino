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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.tag.TagValueConstraint;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes IRC {@link IcebergLabels} from a foreign catalog and re-materializes them as Gravitino
 * tags on the corresponding (federated) table and columns — the consumer counterpart of {@link
 * TagLabelResolver}.
 *
 * <p>This is the technical demonstration that a rich, structured tag model can ingest the flat read
 * field and rebuild usable governance. It implements the reconciliation rules a governed-tag (ABAC)
 * catalog applies when accepting foreign tags:
 *
 * <ul>
 *   <li>key+value matches an existing (governed) tag whose value constraint allows it → assign it;
 *   <li>key matches a governed tag but the value is not allowed → skip (fail open);
 *   <li>no tag exists for the key → create an ungoverned tag (marked with its foreign source) and
 *       assign it;
 *   <li>the association already exists → skip (idempotent, so re-import is safe).
 * </ul>
 *
 * <p>Each incoming label lands as a <em>direct</em> tag on the consumer object: inheritance
 * provenance is not on the wire, and reconciliation is by name/value (there is no stable id to key
 * on), which is exactly the flat-vs-structured boundary. The actual read of {@code labels} off a
 * federated {@code loadTable} response, and the on-demand/cron refresh trigger, are the integration
 * layer on top of this importer.
 */
public class LabelTagImporter {

  private static final Logger LOG = LoggerFactory.getLogger(LabelTagImporter.class);

  /** Tag property recording the foreign catalog an ungoverned tag was imported from. */
  public static final String SOURCE_PROPERTY = "labels.imported-from";

  private final TagDispatcher tagDispatcher;
  private final String metalake;
  private final String separator;

  /**
   * Creates an importer.
   *
   * @param tagDispatcher the tag dispatcher used to create tags and associate values
   * @param metalake the metalake to import tags into
   * @param separator the hierarchical schema separator
   */
  public LabelTagImporter(TagDispatcher tagDispatcher, String metalake, String separator) {
    this.tagDispatcher = tagDispatcher;
    this.metalake = metalake;
    this.separator = separator;
  }

  /**
   * Creates an importer from the running server environment.
   *
   * @return an importer bound to the current environment
   */
  public static LabelTagImporter fromEnv() {
    return new LabelTagImporter(
        GravitinoEnv.getInstance().tagDispatcher(),
        IcebergRESTServerContext.getInstance().metalakeName(),
        HierarchicalSchemaUtil.schemaSeparator());
  }

  /**
   * Imports the labels of a foreign table into Gravitino tags on the corresponding table and
   * columns.
   *
   * @param catalogName the Gravitino catalog the federated table lives in
   * @param tableIdentifier the Iceberg table identifier
   * @param schema the table schema, used to map each label's {@code field-id} to a column
   * @param labels the labels returned by the foreign catalog
   * @param source the foreign catalog name, recorded on tags created during import
   * @return a summary of what was imported and skipped
   */
  public ImportResult importTableLabels(
      String catalogName,
      TableIdentifier tableIdentifier,
      Schema schema,
      IcebergLabels labels,
      String source) {
    ImportResult result = new ImportResult();
    if (labels == null || labels.isEmpty()) {
      return result;
    }

    String schemaName = String.join(separator, tableIdentifier.namespace().levels());
    String tableName = tableIdentifier.name();

    MetadataObject tableObject =
        MetadataObjects.of(
            Arrays.asList(catalogName, schemaName, tableName), MetadataObject.Type.TABLE);
    labels.objectLabels().forEach((key, value) -> importOne(tableObject, key, value, source, result));

    labels
        .fieldLabels()
        .forEach(
            (fieldId, columnLabels) -> {
              Types.NestedField field = schema == null ? null : schema.findField(fieldId);
              if (field == null) {
                columnLabels.forEach(
                    (key, value) ->
                        result.skip(key, value, "no column for field-id " + fieldId));
                return;
              }
              MetadataObject columnObject =
                  MetadataObjects.of(
                      Arrays.asList(catalogName, schemaName, tableName, field.name()),
                      MetadataObject.Type.COLUMN);
              columnLabels.forEach(
                  (key, value) -> importOne(columnObject, key, value, source, result));
            });

    return result;
  }

  private void importOne(
      MetadataObject object, String key, String value, String source, ImportResult result) {
    Tag existing;
    try {
      existing = tagDispatcher.getTag(metalake, key);
    } catch (NoSuchTagException e) {
      existing = null;
    }

    if (existing == null) {
      // No tag for this key: create an ungoverned tag, marked with its foreign source.
      Map<String, String> properties = new HashMap<>();
      if (source != null) {
        properties.put(SOURCE_PROPERTY, source);
      }
      tagDispatcher.createTag(metalake, key, "Imported from foreign catalog", properties);
    } else if (!isValueAllowed(existing.valueConstraint(), value)) {
      // Governed tag whose constraint rejects this value: fail open (do not import).
      result.skip(key, value, "value not allowed by governed tag constraint");
      return;
    }

    TagValue tagValue =
        (value == null || value.isEmpty()) ? TagValue.noValue(key) : TagValue.of(key, value);
    try {
      tagDispatcher.associateTagValuesForMetadataObject(
          metalake, object, new TagValue[] {tagValue}, new TagValue[0]);
      result.imported(key, value);
    } catch (TagAlreadyAssociatedException e) {
      result.skip(key, value, "already associated");
    }
  }

  private static boolean isValueAllowed(TagValueConstraint constraint, String value) {
    if (constraint == null) {
      return true;
    }
    switch (constraint.type()) {
      case ANY_VALUE:
        return true;
      case NO_VALUE:
        return value == null || value.isEmpty();
      case ALLOWED_VALUES:
        return value != null && Arrays.asList(constraint.allowedValues()).contains(value);
      default:
        return false;
    }
  }

  /** Summary of an import: which labels were applied and which were skipped (with a reason). */
  public static final class ImportResult {
    private final List<String> imported = new ArrayList<>();
    private final List<String> skipped = new ArrayList<>();

    private void imported(String key, String value) {
      imported.add(key + "=" + value);
    }

    private void skip(String key, String value, String reason) {
      String entry = key + "=" + value + " (" + reason + ")";
      skipped.add(entry);
      LOG.info("Skipped imported label {}", entry);
    }

    /**
     * @return the labels applied as tags, formatted {@code key=value}.
     */
    public List<String> imported() {
      return imported;
    }

    /**
     * @return the labels skipped, formatted {@code key=value (reason)}.
     */
    public List<String> skipped() {
      return skipped;
    }
  }
}
