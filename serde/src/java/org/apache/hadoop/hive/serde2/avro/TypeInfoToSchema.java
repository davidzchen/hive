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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class TypeInfoToSchema {
  /**
   * Converts a list of Hive TypeInfo and column names into an Avro Schema.
   *
   * @param columnTypes
   * @param columnNames
   * @return Avro schema
   */
  public static Schema convert(List<TypeInfo> columnTypes, List<String> columnNames) {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (int i = 0; i < columnTypes.size(); i++) {
      final Schema fieldSchema = convert(columnTypes.get(i), columnNames.get(i));
      fields.add(new Schema.Field(columnNames.get(i), fieldSchema, null, null));
    }
    Schema recordSchema = Schema.createRecord("schema", null, null, false);
    recordSchema.setFields(fields);
    return recordSchema;
  }

  private static Schema convert(TypeInfo typeInfo, String fieldName) {
    final Category typeCategory = typeInfo.getCategory();
    switch (typeCategory) {
      case PRIMITIVE:
        return convertPrimitive(fieldName, (PrimitiveTypeInfo) typeInfo);
      case LIST:
        return convertList(fieldName, (ListTypeInfo) typeInfo);
      case STRUCT:
        return convertStruct(fieldName, (StructTypeInfo) typeInfo);
      case MAP:
        return convertMap(fieldName, (MapTypeInfo) typeInfo);
      default:
        throw new TypeNotPresentException(typeInfo.getTypeName(), null);
    }
  }

  private static Schema convertPrimitive(String fieldName, PrimitiveTypeInfo typeInfo) {
    if (typeInfo.equals(TypeInfoFactory.intTypeInfo)
        || typeInfo.equals(TypeInfoFactory.shortTypeInfo)
        || typeInfo.equals(TypeInfoFactory.byteTypeInfo)) {
      return Schema.create(Schema.Type.INT);
    } else if (typeInfo.equals(TypeInfoFactory.longTypeInfo)) {
      return Schema.create(Schema.Type.LONG);
    } else if (typeInfo.equals(TypeInfoFactory.floatTypeInfo)) {
      return Schema.create(Schema.Type.FLOAT);
    } else if (typeInfo.equals(TypeInfoFactory.doubleTypeInfo)) {
      return Schema.create(Schema.Type.DOUBLE);
    } else if (typeInfo.equals(TypeInfoFactory.booleanTypeInfo)) {
      return Schema.create(Schema.Type.BOOLEAN);
    } else if (typeInfo.equals(TypeInfoFactory.stringTypeInfo)) {
      return Schema.create(Schema.Type.STRING);
    } else if (typeInfo.equals(TypeInfoFactory.binaryTypeInfo)) {
      return Schema.create(Schema.Type.BYTES);
    } else if (typeInfo.equals(TypeInfoFactory.decimalTypeInfo)) {
      throw new RuntimeException("Decimal type not supported.");
    } else if (typeInfo.equals(TypeInfoFactory.timestampTypeInfo)) {
      throw new RuntimeException("Timestamp type not supported.");
    } else if (typeInfo.equals(TypeInfoFactory.dateTypeInfo)) {
      throw new RuntimeException("Date type not supported.");
    } else if (typeInfo.equals(TypeInfoFactory.varcharTypeInfo)) {
      throw new RuntimeException("Varchar type not supported.");
    } else if (typeInfo.equals(TypeInfoFactory.unknownTypeInfo)) {
      throw new RuntimeException("Unknown type not supported.");
    } else {
      throw new RuntimeException("Unknown type: " + typeInfo);
    }
  }

  private static Schema convertList(String fieldName, ListTypeInfo typeInfo) {
    final TypeInfo elementTypeInfo = typeInfo.getListElementTypeInfo();
    final Schema elementType = convert(elementTypeInfo, "array_element");
    return Schema.createArray(elementType);
  }

  private static Schema convertStruct(String fieldName, StructTypeInfo typeInfo) {
    final List<String> columnNames = typeInfo.getAllStructFieldNames();
    final List<TypeInfo> columnTypeInfos = typeInfo.getAllStructFieldTypeInfos();
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (int i = 0; i < columnNames.size(); i++) {
      final String columnFieldName = columnNames.get(i);
      final Schema fieldSchema = convert(columnTypeInfos.get(i), columnFieldName);
      fields.add(new Schema.Field(columnFieldName, fieldSchema, null, null));
    }
    final Schema recordSchema = Schema.createRecord(fieldName, null, null, false);
    recordSchema.setFields(fields);
    return recordSchema;
  }

  private static Schema convertMap(String fieldName, MapTypeInfo typeInfo) {
    final TypeInfo keyTypeInfo = typeInfo.getMapKeyTypeInfo();
    if (!keyTypeInfo.equals(TypeInfoFactory.stringTypeInfo)) {
      throw new RuntimeException("Avro does not support maps with key type: " + keyTypeInfo);
    }
    final Schema valueType = convert(typeInfo.getMapValueTypeInfo(), "value");
    return Schema.createMap(valueType);
  }
}
