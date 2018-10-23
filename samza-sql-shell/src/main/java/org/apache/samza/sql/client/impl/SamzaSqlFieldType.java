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

package org.apache.samza.sql.client.impl;

import org.apache.samza.sql.client.interfaces.SqlSchema;

/**
 * Types of Samza Sql fields.
 */
public class SamzaSqlFieldType {

  private TypeName typeName;
  private SamzaSqlFieldType elementType;
  private SamzaSqlFieldType valueType;
  private SqlSchema rowSchema;

  private SamzaSqlFieldType(TypeName typeName, SamzaSqlFieldType elementType, SamzaSqlFieldType valueType, SqlSchema rowSchema) {
    this.typeName = typeName;
    this.elementType = elementType;
    this.valueType = valueType;
    this.rowSchema = rowSchema;
  }

  public static SamzaSqlFieldType createPrimitiveFieldType(TypeName typeName) {
    return new SamzaSqlFieldType(typeName, null, null, null);
  }

  public static SamzaSqlFieldType createArrayFieldType(SamzaSqlFieldType elementType) {
    return new SamzaSqlFieldType(TypeName.ARRAY, elementType, null, null);
  }

  public static SamzaSqlFieldType createMapFieldType(SamzaSqlFieldType valueType) {
    return new SamzaSqlFieldType(TypeName.MAP, null, valueType, null);
  }

  public static SamzaSqlFieldType createRowFieldType(SqlSchema rowSchema) {
    return new SamzaSqlFieldType(TypeName.ROW, null, null, rowSchema);
  }

  public boolean isPrimitiveField() {
    return typeName != TypeName.ARRAY && typeName != TypeName.MAP && typeName != TypeName.ROW;
  }

  public TypeName getTypeName() {
    return typeName;
  }

  public SamzaSqlFieldType getElementType() {
    return elementType;
  }

  public SamzaSqlFieldType getValueType() {
    return valueType;
  }

  public SqlSchema getRowSchema() {
    return rowSchema;
  }

  public enum TypeName {
    BYTE, // One-byte signed integer.
    INT16, // two-byte signed integer.
    INT32, // four-byte signed integer.
    INT64, // eight-byte signed integer.
    DECIMAL, // Decimal integer
    FLOAT,
    DOUBLE,
    STRING, // String.
    DATETIME, // Date and time.
    BOOLEAN, // Boolean.
    BYTES, // Byte array.
    ARRAY,
    MAP,
    ROW, // The field is itself a nested row.
    ANY
  }
}
