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

  private final SamzaSqlFieldType fieldType;
  private final SqlFieldSchema elementType;
  private final SqlFieldSchema valueType;
  private final SqlSchema rowSchema;
  private final Boolean isNullable;
  private final Boolean hasDefaultValue;

  private SqlFieldSchema(SamzaSqlFieldType fieldType, SqlFieldSchema elementType, SqlFieldSchema valueType,
      SqlSchema rowSchema, boolean isNullable, boolean hasDefaultValue) {
    this.fieldType = fieldType;
    this.elementType = elementType;
    this.valueType = valueType;
    this.rowSchema = rowSchema;
    this.isNullable = isNullable;
    this.hasDefaultValue = hasDefaultValue;
  }

  /**
   * Create a primitive field schema.
   * @param typeName
   * @return
   */
  public static SqlFieldSchema createPrimitiveSchema(SamzaSqlFieldType typeName, boolean isNullable,
      boolean hasDefaultValue) {
    return new SqlFieldSchema(typeName, null, null, null, isNullable, hasDefaultValue);
  }

  public static SqlFieldSchema createArraySchema(SqlFieldSchema elementType, boolean isNullable,
      boolean hasDefaultValue) {
    return new SqlFieldSchema(SamzaSqlFieldType.ARRAY, elementType, null, null, isNullable, hasDefaultValue);
  }

  public static SqlFieldSchema createMapSchema(SqlFieldSchema valueType, boolean isNullable, boolean hasDefaultValue) {
    return new SqlFieldSchema(SamzaSqlFieldType.MAP, null, valueType, null, isNullable, hasDefaultValue);
  }

  public static SqlFieldSchema createRowFieldSchema(SqlSchema rowSchema, boolean isNullable, boolean hasDefaultValue) {
    return new SqlFieldSchema(SamzaSqlFieldType.ROW, null, null, rowSchema, isNullable, hasDefaultValue);
  }

  /**
   * @return whether the field is a primitive field type or not.
   */
  public boolean isPrimitiveField() {
    return fieldType != SamzaSqlFieldType.ARRAY && fieldType != SamzaSqlFieldType.MAP &&
        fieldType != SamzaSqlFieldType.ROW;
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

  /**
   * Get if the field type is nullable.
   */
  public boolean isNullable() {
    return isNullable;
  }

  /**
   * Get if the field has default value.
   */
  public boolean hasDefaultValue() {
    return hasDefaultValue;
  }
}
