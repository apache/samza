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

package org.apache.samza.sql.data.avro;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.sql.api.data.Data;
import org.apache.samza.sql.api.data.Schema;


public class AvroData implements Data {
  protected final Object datum;
  protected final AvroSchema schema;

  private AvroData(AvroSchema schema, Object datum) {
    this.datum = datum;
    this.schema = schema;
  }

  @Override
  public Schema schema() {
    return this.schema;
  }

  @Override
  public Object value() {
    return this.datum;
  }

  @Override
  public int intValue() {
    throw new UnsupportedOperationException("Can't get value for an unknown data type.");
  }

  @Override
  public long longValue() {
    throw new UnsupportedOperationException("Can't get value for an unknown data type.");
  }

  @Override
  public float floatValue() {
    throw new UnsupportedOperationException("Can't get value for an unknown data type.");
  }

  @Override
  public double doubleValue() {
    throw new UnsupportedOperationException("Can't get value for an unknown data type.");
  }

  @Override
  public boolean booleanValue() {
    throw new UnsupportedOperationException("Can't get value for an unknown data type.");
  }

  @Override
  public String strValue() {
    throw new UnsupportedOperationException("Can't get value for an unknown data type.");
  }

  @Override
  public byte[] bytesValue() {
    throw new UnsupportedOperationException("Can't get value for an unknown data type.");
  }

  @Override
  public List<Object> arrayValue() {
    throw new UnsupportedOperationException("Can't get value for an unknown data type.");
  }

  @Override
  public Map<Object, Object> mapValue() {
    throw new UnsupportedOperationException("Can't get value for an unknown data type.");
  }

  @Override
  public Data getElement(int index) {
    throw new UnsupportedOperationException("Can't get value for an unknown data type.");
  }

  @Override
  public Data getFieldData(String fldName) {
    throw new UnsupportedOperationException("Can't get value for an unknown data type.");
  }

  public static AvroData getArray(AvroSchema schema, Object datum) {
    if (schema.getType() != Schema.Type.ARRAY) {
      throw new IllegalArgumentException("Can't create an array object with non-array schema:" + schema.getType());
    }
    return new AvroData(schema, datum) {
      @SuppressWarnings("unchecked")
      private final GenericArray<Object> array = (GenericArray<Object>) this.datum;

      @Override
      public List<Object> arrayValue() {
        return this.array;
      }

      @Override
      public Data getElement(int index) {
        return this.schema.getElementType().read(array.get(index));
      }

    };
  }

  public static AvroData getMap(AvroSchema schema, Object datum) {
    if (schema.getType() != Schema.Type.MAP) {
      throw new IllegalArgumentException("Can't create a map object with non-map schema:" + schema.getType());
    }
    return new AvroData(schema, datum) {
      @SuppressWarnings("unchecked")
      private final Map<Object, Object> map = (Map<Object, Object>) datum;

      @Override
      public Map<Object, Object> mapValue() {
        return this.map;
      }

      @Override
      public Data getFieldData(String fldName) {
        // TODO Auto-generated method stub
        return this.schema.getValueType().read(map.get(fldName));
      }

    };
  }

  public static AvroData getStruct(AvroSchema schema, Object datum) {
    if (schema.getType() != Schema.Type.STRUCT) {
      throw new IllegalArgumentException("Can't create a struct object with non-struct schema:" + schema.getType());
    }
    return new AvroData(schema, datum) {
      private final GenericRecord record = (GenericRecord) datum;

      @Override
      public Data getFieldData(String fldName) {
        // TODO Auto-generated method stub
        return this.schema.getFieldType(fldName).read(record.get(fldName));
      }

    };
  }

  public static AvroData getInt(AvroSchema schema, Object datum) {
    if (schema.getType() != Schema.Type.INTEGER || !(datum instanceof Integer)) {
      throw new IllegalArgumentException("data object and schema mismatch. schema:" + schema.getType() + ", data: "
          + datum.getClass().getName());
    }
    return new AvroData(schema, datum) {
      @Override
      public int intValue() {
        return ((Integer) datum).intValue();
      }

    };
  }

  public static AvroData getLong(AvroSchema schema, Object datum) {
    if (schema.getType() != Schema.Type.LONG || !(datum instanceof Long)) {
      throw new IllegalArgumentException("data object and schema mismatch. schema:" + schema.getType() + ", data: "
          + datum.getClass().getName());
    }
    return new AvroData(schema, datum) {
      @Override
      public long longValue() {
        return ((Long) datum).longValue();
      }

    };
  }

  public static AvroData getFloat(AvroSchema schema, Object datum) {
    if (schema.getType() != Schema.Type.FLOAT || !(datum instanceof Float)) {
      throw new IllegalArgumentException("data object and schema mismatch. schema:" + schema.getType() + ", data: "
          + datum.getClass().getName());
    }
    return new AvroData(schema, datum) {
      @Override
      public float floatValue() {
        return ((Float) datum).floatValue();
      }

    };
  }

  public static AvroData getDouble(AvroSchema schema, Object datum) {
    if (schema.getType() != Schema.Type.DOUBLE || !(datum instanceof Double)) {
      throw new IllegalArgumentException("data object and schema mismatch. schema:" + schema.getType() + ", data: "
          + datum.getClass().getName());
    }
    return new AvroData(schema, datum) {
      @Override
      public double doubleValue() {
        return ((Double) datum).doubleValue();
      }

    };
  }

  public static AvroData getBoolean(AvroSchema schema, Object datum) {
    if (schema.getType() != Schema.Type.BOOLEAN || !(datum instanceof Boolean)) {
      throw new IllegalArgumentException("data object and schema mismatch. schema:" + schema.getType() + ", data: "
          + datum.getClass().getName());
    }
    return new AvroData(schema, datum) {
      @Override
      public boolean booleanValue() {
        return ((Boolean) datum).booleanValue();
      }

    };
  }

  public static AvroData getString(AvroSchema schema, Object datum) {
    if (schema.getType() != Schema.Type.STRING || !(datum instanceof CharSequence)) {
      throw new IllegalArgumentException("data object and schema mismatch. schema:" + schema.getType() + ", data: "
          + datum.getClass().getName());
    }
    return new AvroData(schema, datum) {
      @Override
      public String strValue() {
        return ((CharSequence) datum).toString();
      }

    };
  }

  public static AvroData getBytes(AvroSchema schema, Object datum) {
    if (schema.getType() != Schema.Type.BYTES || !(datum instanceof ByteBuffer)) {
      throw new IllegalArgumentException("data object and schema mismatch. schema:" + schema.getType() + ", data: "
          + datum.getClass().getName());
    }
    return new AvroData(schema, datum) {
      @Override
      public byte[] bytesValue() {
        return ((ByteBuffer) datum).array();
      }

    };
  }
}
