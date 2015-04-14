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

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.samza.sql.api.data.Data;
import org.apache.samza.sql.api.data.Schema;


public class AvroSchema implements Schema {

  protected final org.apache.avro.Schema avroSchema;
  protected final Schema.Type type;

  private final static Map<org.apache.avro.Schema.Type, AvroSchema> primSchemas =
      new HashMap<org.apache.avro.Schema.Type, AvroSchema>();

  static {
    primSchemas.put(org.apache.avro.Schema.Type.INT,
        new AvroSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)) {
      @Override
      public Data read(Object datum) {
        return AvroData.getInt(this, datum);
      }
    });
    primSchemas.put(org.apache.avro.Schema.Type.LONG,
        new AvroSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)) {
      @Override
      public Data read(Object datum) {
        return AvroData.getLong(this, datum);
      }
    });
    primSchemas.put(org.apache.avro.Schema.Type.FLOAT,
        new AvroSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT)) {
      @Override
      public Data read(Object datum) {
        return AvroData.getFloat(this, datum);
      }
    });
    primSchemas.put(org.apache.avro.Schema.Type.DOUBLE,
        new AvroSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE)) {
      @Override
      public Data read(Object datum) {
        return AvroData.getDouble(this, datum);
      }
    });
    primSchemas.put(org.apache.avro.Schema.Type.BOOLEAN,
        new AvroSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN)) {
      @Override
      public Data read(Object datum) {
        return AvroData.getBoolean(this, datum);
      }
    });
    primSchemas.put(org.apache.avro.Schema.Type.STRING,
        new AvroSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)) {
      @Override
      public Data read(Object datum) {
        return AvroData.getString(this, datum);
      }
    });
    primSchemas.put(org.apache.avro.Schema.Type.BYTES,
        new AvroSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES)) {
      @Override
      public Data read(Object datum) {
        return AvroData.getBytes(this, datum);
      }
    });
  };

  public static AvroSchema getSchema(final org.apache.avro.Schema schema) {
    Schema.Type type = mapType(schema.getType());
    if (type != Schema.Type.ARRAY && type != Schema.Type.MAP && type != Schema.Type.STRUCT) {
      return primSchemas.get(schema.getType());
    }
    // otherwise, construct the new schema
    // TODO: It would be possible to assign each complex schema an ID and cache it w/o repeated create in-memory schema objects
    switch (type) {
      case ARRAY:
        return new AvroSchema(schema) {
          @Override
          public Data transform(Data input) {
            // This would get all the elements until the length of the current schema's array length
            if (input.schema().getType() != Schema.Type.ARRAY) {
              throw new IllegalArgumentException("Schema mismatch. Can't transfer data. input schema: "
                  + input.schema().getType());
            }
            if (!input.schema().getElementType().equals(this.getElementType())) {
              throw new IllegalArgumentException("Element schema mismatch. Can't transfer data. input schema: "
                  + input.schema().getElementType().getType());
            }
            // input type matches array type
            return AvroData.getArray(this, input.value());
          }
        };
      case MAP:
        return new AvroSchema(schema) {
          @Override
          public Data transform(Data input) {
            // This would get all the elements until the length of the current schema's array length
            if (input.schema().getType() != Schema.Type.MAP) {
              throw new IllegalArgumentException("Schema mismatch. Can't transfer data. input schema: "
                  + input.schema().getType());
            }
            if (!input.schema().getValueType().equals(this.getValueType())) {
              throw new IllegalArgumentException("Element schema mismatch. Can't transfer data. input schema: "
                  + input.schema().getValueType().getType());
            }
            // input type matches map type
            return AvroData.getMap(this, input.value());
          }
        };
      case STRUCT:
        return new AvroSchema(schema) {
          @SuppressWarnings("serial")
          private final Map<String, Schema> fldSchemas = new HashMap<String, Schema>() {
            {
              for (Field field : schema.getFields()) {
                put(field.name(), getSchema(field.schema()));
              }
            }
          };

          @Override
          public Map<String, Schema> getFields() {
            return this.fldSchemas;
          }

          @Override
          public Schema getFieldType(String fldName) {
            return this.fldSchemas.get(fldName);
          }

          @Override
          public Data transform(Data input) {
            // This would get all the elements until the length of the current schema's array length
            if (input.schema().getType() != Schema.Type.STRUCT) {
              throw new IllegalArgumentException("Schema mismatch. Can't transfer data. input schema: "
                  + input.schema().getType());
            }
            // Note: this particular transform function only implements "projection to a sub-set" concept.
            // More complex function is needed if some other concepts such as "merge from two sets of data", "allow null if does not exist" are needed
            for (String fldName : this.fldSchemas.keySet()) {
              // check each field schema matches input
              Schema fldSchema = this.fldSchemas.get(fldName);
              Schema inputFld = input.schema().getFieldType(fldName);
              if (!fldSchema.equals(inputFld)) {
                throw new IllegalArgumentException("Field schema mismatch. Can't transfer data for field " + fldName
                    + ". input field schema:" + inputFld.getType() + ", this field schema: " + fldSchema.getType());
              }
            }
            // input type matches struct type
            return AvroData.getStruct(this, input.value());
          }

        };
      default:
        throw new IllegalArgumentException("Un-recognized complext data type:" + type);
    }
  }

  private AvroSchema(org.apache.avro.Schema schema) {
    this.avroSchema = schema;
    this.type = mapType(schema.getType());
  }

  private static Type mapType(org.apache.avro.Schema.Type type) {
    switch (type) {
      case ARRAY:
        return Schema.Type.ARRAY;
      case RECORD:
        return Schema.Type.STRUCT;
      case MAP:
        return Schema.Type.MAP;
      case INT:
        return Schema.Type.INTEGER;
      case LONG:
        return Schema.Type.LONG;
      case BOOLEAN:
        return Schema.Type.BOOLEAN;
      case FLOAT:
        return Schema.Type.FLOAT;
      case DOUBLE:
        return Schema.Type.DOUBLE;
      case STRING:
        return Schema.Type.STRING;
      case BYTES:
        return Schema.Type.BYTES;
      default:
        throw new IllegalArgumentException("Avro schema: " + type + " is not supported");
    }
  }

  @Override
  public Type getType() {
    return this.type;
  }

  @Override
  public Schema getElementType() {
    if (this.type != Schema.Type.ARRAY) {
      throw new UnsupportedOperationException("Can't getElmentType with non-array schema: " + this.type);
    }
    return getSchema(this.avroSchema.getElementType());
  }

  @Override
  public Schema getValueType() {
    if (this.type != Schema.Type.MAP) {
      throw new UnsupportedOperationException("Can't getValueType with non-map schema: " + this.type);
    }
    return getSchema(this.avroSchema.getValueType());
  }

  @Override
  public Map<String, Schema> getFields() {
    throw new UnsupportedOperationException("Can't get field types with unknown schema type:" + this.type);
  }

  @Override
  public Schema getFieldType(String fldName) {
    throw new UnsupportedOperationException("Can't getFieldType with non-map/non-struct schema: " + this.type);
  }

  @Override
  public Data read(Object object) {
    if (this.avroSchema.getType() == org.apache.avro.Schema.Type.ARRAY) {
      return AvroData.getArray(this, object);
    } else if (this.avroSchema.getType() == org.apache.avro.Schema.Type.MAP) {
      return AvroData.getMap(this, object);
    } else if (this.avroSchema.getType() == org.apache.avro.Schema.Type.RECORD) {
      return AvroData.getStruct(this, object);
    }
    throw new UnsupportedOperationException("Reading unknown complext type:" + this.type + " is not supported");
  }

  @Override
  public Data transform(Data inputData) {
    if (inputData.schema().getType() == Schema.Type.ARRAY || inputData.schema().getType() == Schema.Type.MAP
        || inputData.schema().getType() == Schema.Type.STRUCT) {
      throw new IllegalArgumentException("Complex schema should have overriden the default transform() function.");
    }
    if (inputData.schema().getType() != this.type) {
      throw new IllegalArgumentException("Can't transform a mismatched primitive type. this type:" + this.type
          + ", input type:" + inputData.schema().getType());
    }
    return inputData;
  }

  @Override
  public boolean equals(Schema other) {
    // TODO Auto-generated method stub
    if (this.type != other.getType()) {
      return false;
    }
    switch (this.type) {
      case ARRAY:
        // check if element types are the same
        return this.getElementType().equals(other.getElementType());
      case MAP:
        // check if value types are the same
        return this.getValueType().equals(other.getValueType());
      case STRUCT:
        // check if the fields schemas in this equals the other
        // NOTE: this equals check is in consistent with the "projection to subset" concept implemented in transform()
        for (String fieldName : this.getFields().keySet()) {
          if (!this.getFieldType(fieldName).equals(other.getFieldType(fieldName))) {
            return false;
          }
        }
        return true;
      default:
        return true;
    }
  }

}
