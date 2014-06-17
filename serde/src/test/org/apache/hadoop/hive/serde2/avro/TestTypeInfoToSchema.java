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

package org.apache.hadoop.hive.serde2.avro;

import com.google.common.io.Resources;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTypeInfoToSchema {
  @Test
  public void testAllTypes() throws Exception {
    String fieldNames =
        "myboolean,"
        + "myint,"
        + "mylong,"
        + "myfloat,"
        + "mydouble,"
        + "mybytes,"
        + "mystring,"
        + "myrecord,"
        + "myarray,"
        + "mymap";
    String fieldTypes =
        "boolean,"
        + "int,"
        + "bigint,"
        + "float,"
        + "double,"
        + "binary,"
        + "string,"
        + "struct<mynestedint:int>,"
        + "array<int>,"
        + "map<string,int>";

    List<String> columnNames = Arrays.asList(fieldNames.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(fieldTypes);

    assertEquals(columnNames.size(), columnTypes.size());

    Schema avroSchema = TypeInfoToSchema.convert(columnTypes, columnNames);
    Schema expectedSchema = new Schema.Parser().parse(
        Resources.getResource("alltypes.avsc").openStream());

    assertEquals("Expected: " + expectedSchema.toString(true)
        + "\nGot: " + avroSchema.toString(true),
        expectedSchema.toString(), avroSchema.toString());
  }
}
