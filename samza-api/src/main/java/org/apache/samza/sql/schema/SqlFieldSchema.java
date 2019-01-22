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
 * Schema for the Samza SQL Field.
 */
public class SqlFieldSchema {

  private SamzaSqlFieldType fieldType;
  private SqlFieldSchema elementType;
  private SqlFieldSchema valueType;
  private SqlSchema rowSchema;

  private SqlFieldSchema(SamzaSqlFieldType fieldType, SqlFieldSchema elementType, SqlFieldSchema valueType, SqlSchema rowSchema) {
    this.fieldType = fieldType;
    this.elementType = elementType;
    this.valueType = valueType;
    this.rowSchema = rowSchema;
  }

  /**
   * Create a primitive fi
   * @param typeName
   * @return
   */
  public static SqlFieldSchema createPrimitiveSchema(SamzaSqlFieldType typeName) {
    return new SqlFieldSchema(typeName, null, null, null);
  }

  public static SqlFieldSchema createArraySchema(SqlFieldSchema elementType) {
    return new SqlFieldSchema(SamzaSqlFieldType.ARRAY, elementType, null, null);
  }

  public static SqlFieldSchema createMapSchema(SqlFieldSchema valueType) {
    return new SqlFieldSchema(SamzaSqlFieldType.MAP, null, valueType, null);
  }

  public static SqlFieldSchema createRowFieldSchema(SqlSchema rowSchema) {
    return new SqlFieldSchema(SamzaSqlFieldType.ROW, null, null, rowSchema);
  }

  /**
   * @return whether the field is a primitive field type or not.
   */
  public boolean isPrimitiveField() {
    return fieldType != SamzaSqlFieldType.ARRAY && fieldType != SamzaSqlFieldType.MAP && fieldType != SamzaSqlFieldType.ROW;
  }

  /**
   * Get teh Type of the Samza SQL Field.
   * @return
   */
  public SamzaSqlFieldType getFieldType() {
    return fieldType;
  }

  /**
   * Get the element schema if the field type is {@link SamzaSqlFieldType#ARRAY}
   */
  public SqlFieldSchema getElementSchema() {
    return elementType;
  }

  /**
   * Get the schema of the value if the field type is {@link SamzaSqlFieldType#MAP}
   */
  public SqlFieldSchema getValueScehma() {
    return valueType;
  }

  /**
   * Get the row schema if the field type is {@link SamzaSqlFieldType#ROW}
   */
  public SqlSchema getRowSchema() {
    return rowSchema;
  }


}
