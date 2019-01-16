/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.sql.schema;

/**
 * Types of Samza Sql fields.
 */
public class SqlFieldSchema {

  private SamzaSqlFieldType typeName;
  private SqlFieldSchema elementType;
  private SqlFieldSchema valueType;
  private SqlSchema rowSchema;

  private SqlFieldSchema(SamzaSqlFieldType typeName, SqlFieldSchema elementType, SqlFieldSchema valueType, SqlSchema rowSchema) {
    this.typeName = typeName;
    this.elementType = elementType;
    this.valueType = valueType;
    this.rowSchema = rowSchema;
  }

  public static SqlFieldSchema createPrimitiveFieldType(SamzaSqlFieldType typeName) {
    return new SqlFieldSchema(typeName, null, null, null);
  }

  public static SqlFieldSchema createArrayFieldType(SqlFieldSchema elementType) {
    return new SqlFieldSchema(SamzaSqlFieldType.ARRAY, elementType, null, null);
  }

  public static SqlFieldSchema createMapFieldType(SqlFieldSchema valueType) {
    return new SqlFieldSchema(SamzaSqlFieldType.MAP, null, valueType, null);
  }

  public static SqlFieldSchema createRowFieldType(SqlSchema rowSchema) {
    return new SqlFieldSchema(SamzaSqlFieldType.ROW, null, null, rowSchema);
  }

  public boolean isPrimitiveField() {
    return typeName != SamzaSqlFieldType.ARRAY && typeName != SamzaSqlFieldType.MAP && typeName != SamzaSqlFieldType.ROW;
  }

  public SamzaSqlFieldType getTypeName() {
    return typeName;
  }

  public SqlFieldSchema getElementType() {
    return elementType;
  }

  public SqlFieldSchema getValueType() {
    return valueType;
  }

  public SqlSchema getRowSchema() {
    return rowSchema;
  }


}
