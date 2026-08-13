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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagAssignment;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

/**
 * Resolves Gravitino tags into the flat IRC {@link IcebergLabels} projection for the load read
 * path.
 *
 * <p>The producing catalog does the work the flat wire field cannot: it merges directly-attached
 * and inherited tags (child-wins) into {@code object-labels}, and maps column tags onto their
 * Iceberg {@code field-id}. The consumer therefore receives resolved values with no inheritance
 * provenance on the wire — the demonstration that a rich tag model projects cleanly into the flat
 * field.
 */
public class TagLabelResolver {

  private final TagDispatcher tagDispatcher;
  private final String metalake;
  private final String separator;

  /**
   * Creates a resolver.
   *
   * @param tagDispatcher the tag dispatcher used to list associated tags
   * @param metalake the metalake this Iceberg REST server serves
   * @param separator the hierarchical schema separator
   */
  public TagLabelResolver(TagDispatcher tagDispatcher, String metalake, String separator) {
    this.tagDispatcher = tagDispatcher;
    this.metalake = metalake;
    this.separator = separator;
  }

  /**
   * Creates a resolver from the running server environment (tag dispatcher, metalake, and
   * configured schema separator).
   *
   * @return a resolver bound to the current environment
   */
  public static TagLabelResolver fromEnv() {
    return new TagLabelResolver(
        GravitinoEnv.getInstance().tagDispatcher(),
        IcebergRESTServerContext.getInstance().metalakeName(),
        HierarchicalSchemaUtil.schemaSeparator());
  }

  /**
   * Resolves the labels for a table: object-level labels (direct plus inherited from schema and
   * catalog, child-wins) and per-column labels keyed by {@code field-id}.
   *
   * @param catalogName the Gravitino catalog name serving this table
   * @param tableIdentifier the Iceberg table identifier
   * @param schema the current table schema, used to map column names to {@code field-id}
   * @return the resolved labels (possibly {@link IcebergLabels#isEmpty() empty})
   */
  public IcebergLabels resolveTableLabels(
      String catalogName, TableIdentifier tableIdentifier, Schema schema) {
    String schemaName = String.join(separator, tableIdentifier.namespace().levels());
    String tableName = tableIdentifier.name();

    MetadataObject tableObject =
        MetadataObjects.of(
            Arrays.asList(catalogName, schemaName, tableName), MetadataObject.Type.TABLE);

    // Object-level: apply ancestors farthest-first, then the table's own tags, so the
    // most-specific level wins on key collisions. getParentMetadataObjects is nearest-first.
    Map<String, String> objectLabels = IcebergLabels.newLabelMap();
    List<MetadataObject> ancestors = MetadataObjectUtil.getParentMetadataObjects(tableObject);
    for (int i = ancestors.size() - 1; i >= 0; i--) {
      putTags(objectLabels, listTags(ancestors.get(i)));
    }
    putTags(objectLabels, listTags(tableObject));

    // Field-level: direct column tags only, keyed by field-id.
    Map<Integer, Map<String, String>> fieldLabels = new LinkedHashMap<>();
    if (schema != null) {
      for (Types.NestedField column : schema.columns()) {
        MetadataObject columnObject =
            MetadataObjects.of(
                Arrays.asList(catalogName, schemaName, tableName, column.name()),
                MetadataObject.Type.COLUMN);
        Map<String, String> columnLabels = IcebergLabels.newLabelMap();
        putTags(columnLabels, listTags(columnObject));
        if (!columnLabels.isEmpty()) {
          fieldLabels.put(column.fieldId(), columnLabels);
        }
      }
    }

    return new IcebergLabels(objectLabels, fieldLabels);
  }

  private Tag[] listTags(MetadataObject object) {
    try {
      return tagDispatcher.listTagsInfoForMetadataObject(metalake, object);
    } catch (NoSuchMetadataObjectException e) {
      // Object not resolvable as a Gravitino entity (e.g. non-Gravitino backend) — no labels.
      return new Tag[0];
    }
  }

  private static void putTags(Map<String, String> target, Tag[] tags) {
    if (tags == null) {
      return;
    }
    for (Tag tag : tags) {
      target.put(tag.name(), assignmentValue(tag));
    }
  }

  private static String assignmentValue(Tag tag) {
    return tag.assignment()
        .filter(TagAssignment::hasValues)
        .map(assignment -> String.join(",", assignment.values()))
        .orElse("");
  }
}
