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
package org.apache.samza.sql.metadata;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts an Avro schema to Calcite RelDataType.
 * <p/>
 * <p>Inspired by parquet-mr.</p>
 */
public class AvroSchemaConverter {

  private final RelDataTypeFactory relDataTypeFactory;
  private final Schema rootSchema;

  public AvroSchemaConverter(RelDataTypeFactory relDataTypeFactory, Schema schema) {
    this.relDataTypeFactory = relDataTypeFactory;
    this.rootSchema = schema;
  }

  public RelDataType convert() {
    // At the top level only records are supported
    if (rootSchema.getType() != Schema.Type.RECORD) {
      throw new RuntimeException(
          String.format("Type: %s is unsupported at this level; Only Record type of supported at top level!",
              rootSchema.getType()));
    }

    return convertRecord(rootSchema, true);
  }

  private RelDataType convertRecord(Schema recordSchema, boolean isRoot) {
    RelDataTypeFactory.FieldInfoBuilder builder = relDataTypeFactory.builder();

    for (Schema.Field field : recordSchema.getFields()) {
      Schema fieldSchema = field.schema();
      if (fieldSchema.getType() == Schema.Type.NULL) {
        continue;
      }

      convertField(builder, field.name(), fieldSchema);
    }

    RelDataType record = builder.build();
    if(isRoot) {
      // Record at root level is treated differently.
      return record;
    }

    return relDataTypeFactory.createStructType(record.getFieldList());
  }

  private void convertField(RelDataTypeFactory.FieldInfoBuilder builder,
                            String fieldName,
                            Schema fieldSchema) {
    builder.add(fieldName, convertFieldType(fieldSchema));
  }

  private RelDataType convertFieldType(Schema elementType) {
    Schema.Type type = elementType.getType();
    if (type == Schema.Type.STRING) {
      return relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR);
    } else if (type == Schema.Type.INT) {
      return relDataTypeFactory.createSqlType(SqlTypeName.INTEGER);
    } else if (type == Schema.Type.BOOLEAN) {
      return relDataTypeFactory.createSqlType(SqlTypeName.BOOLEAN);
    } else if (type == Schema.Type.BYTES) {
      return relDataTypeFactory.createSqlType(SqlTypeName.BINARY);
    } else if (type == Schema.Type.LONG) {
      return relDataTypeFactory.createSqlType(SqlTypeName.BIGINT);
    } else if (type == Schema.Type.DOUBLE) {
      return relDataTypeFactory.createSqlType(SqlTypeName.DOUBLE);
    } else if (type == Schema.Type.FLOAT) {
      return relDataTypeFactory.createSqlType(SqlTypeName.FLOAT);
    } else if (type == Schema.Type.ARRAY) {
      return relDataTypeFactory.createArrayType(convertFieldType(elementType), -1);
    } else if (type == Schema.Type.RECORD) {
      return convertRecord(elementType, false);
    } else if (type == Schema.Type.MAP) {
      return relDataTypeFactory.createMapType(relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
          convertFieldType(elementType.getValueType()));
    } else if (type == Schema.Type.FIXED) {
      return relDataTypeFactory.createSqlType(SqlTypeName.VARBINARY, elementType.getFixedSize());
    } else if (type == Schema.Type.UNION) {
      List<Schema> types = elementType.getTypes();
      List<Schema> nonNullTypes = new ArrayList<Schema>();
      boolean foundNull = false;

      for(Schema s : types) {
        if(s.getType() == Schema.Type.NULL){
          foundNull = true;
        } else {
          nonNullTypes.add(s);
        }
      }

      if(nonNullTypes.size() > 1){
        throw new RuntimeException("Multiple non null types in a union is not supported.");
      } else {
        return relDataTypeFactory.createTypeWithNullability(convertFieldType(nonNullTypes.get(0)), foundNull);
      }
    } else if(type == Schema.Type.ENUM) {
      // TODO: May be there is a better way to handle enums
      relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    return relDataTypeFactory.createSqlType(SqlTypeName.ANY);
  }
}
