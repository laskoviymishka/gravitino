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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergLabels {

  @Test
  void jsonRoundTrip() {
    Map<String, String> objectLabels = new LinkedHashMap<>();
    objectLabels.put("data_classification", "restricted");
    objectLabels.put("owner_team", "data-platform");
    Map<String, String> columnLabels = new LinkedHashMap<>();
    columnLabels.put("pii", "ssn");
    Map<Integer, Map<String, String>> fieldLabels = new LinkedHashMap<>();
    fieldLabels.put(7, columnLabels);

    IcebergLabels original = new IcebergLabels(objectLabels, fieldLabels);
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode json = original.toJson(mapper);
    IcebergLabels parsed = IcebergLabels.fromJson(json);

    Assertions.assertEquals(objectLabels, parsed.objectLabels());
    Assertions.assertEquals(fieldLabels, parsed.fieldLabels());
  }

  @Test
  void fromJsonHandlesNullAndEmpty() {
    Assertions.assertTrue(IcebergLabels.fromJson(null).isEmpty());
    Assertions.assertTrue(
        IcebergLabels.fromJson(new ObjectMapper().createObjectNode()).isEmpty());
  }
}
