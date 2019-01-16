package org.apache.samza.sql.schema;

public enum SamzaSqlFieldType {
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
