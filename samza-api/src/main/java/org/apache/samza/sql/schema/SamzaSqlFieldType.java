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
 * Type of the Samza SQL field
 */
public enum SamzaSqlFieldType {
  BYTE, // One-byte signed integer.
  INT16, // two-byte signed integer.
  INT32, // four-byte signed integer.
  INT64, // eight-byte signed integer.
  DECIMAL, // Decimal integer
  REAL, // 4 bytes
  FLOAT, // 8 bytes
  DOUBLE, // 8 bytes
  STRING, // String.
  DATETIME, // Date and time.
  BOOLEAN, // Boolean.
  BYTES, // Byte array.
  ARRAY,
  MAP,
  ROW, // The field is itself a nested row.
  ANY
}
