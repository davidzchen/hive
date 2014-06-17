/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce.storage;

import com.google.common.io.Resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.avro.TypeInfoToSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Custom logic for running tests against the Avro storage format.
 */
public class AvroStorageCustomHandler extends StorageCustomHandler {
  /**
   * Takes the schema of the table and converts it to an Avro schema in order to set the required
   * `avro.schema.literal` property of the table.
   *
   * @param table The table object.
   */
  @Override
  public void setCustomTableProperties(Table table) {
    StorageDescriptor sd = table.getSd();
    List<FieldSchema> cols = sd.getCols();
    List<String> columnNames = new ArrayList<String>();
    List<TypeInfo> columnTypes = new ArrayList<TypeInfo>();
    for (FieldSchema col : cols) {
      columnNames.add(col.getName());
      columnTypes.add(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()));
    }
    Schema avroSchema = TypeInfoToSchema.convert(columnTypes, columnNames);

    Map<String, String> tableParams = new HashMap<String, String>();
    tableParams.put("avro.schema.literal", avroSchema.toString());
    table.setParameters(tableParams);
  }
}

