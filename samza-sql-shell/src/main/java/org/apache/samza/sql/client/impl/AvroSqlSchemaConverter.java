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

import com.google.common.base.Joiner;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.client.interfaces.SqlSchema;
import org.apache.samza.sql.client.interfaces.SqlSchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema converter which converts Avro schema to Samza Sql schema
 */
public class AvroSqlSchemaConverter {
  private static final Logger LOG = LoggerFactory.getLogger(AvroSqlSchemaConverter.class);

  public static SqlSchema convertAvroToSamzaSqlSchema(String schema) {
    Schema avroSchema = Schema.parse(schema);
    return getSchema(avroSchema.getFields());
  }

  private static SqlSchema getSchema(List<Schema.Field> fields) {
    SqlSchemaBuilder schemaBuilder = SqlSchemaBuilder.builder();
    for (Schema.Field field : fields) {
      schemaBuilder.addField(field.name(), getColumnTypeName(getFieldType(field.schema())));
    }
    return schemaBuilder.toSchema();
  }

  private static String getColumnTypeName(SamzaSqlFieldType fieldType) {
    if (fieldType.isPrimitiveField()) {
      return fieldType.getTypeName().toString();
    } else if (fieldType.getTypeName() == SamzaSqlFieldType.TypeName.MAP) {
      return String.format("MAP(%s)", getColumnTypeName(fieldType.getValueType()));
    } else if (fieldType.getTypeName() == SamzaSqlFieldType.TypeName.ARRAY) {
      return String.format("ARRAY(%s)", getColumnTypeName(fieldType.getElementType()));
    } else {
      SqlSchema schema = fieldType.getRowSchema();
      List<String> fieldTypes = IntStream.range(0, schema.getFieldCount())
          .mapToObj(i -> schema.getFieldName(i) + " " + schema.getFieldTypeName(i))
          .collect(Collectors.toList());
      String rowSchemaValue = Joiner.on(", ").join(fieldTypes);
      return String.format("STRUCT(%s)", rowSchemaValue);
    }
  }

  private static SamzaSqlFieldType getFieldType(org.apache.avro.Schema schema) {
    switch (schema.getType()) {
      case ARRAY:
        return SamzaSqlFieldType.createArrayFieldType(getFieldType(schema.getElementType()));
      case BOOLEAN:
        return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.BOOLEAN);
      case DOUBLE:
        return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.DOUBLE);
      case FLOAT:
        return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.FLOAT);
      case ENUM:
        return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.STRING);
      case UNION:
        // NOTE: We only support Union types when they are used for representing Nullable fields in Avro
        List<org.apache.avro.Schema> types = schema.getTypes();
        if (types.size() == 2) {
          if (types.get(0).getType() == org.apache.avro.Schema.Type.NULL) {
            return getFieldType(types.get(1));
          } else if ((types.get(1).getType() == org.apache.avro.Schema.Type.NULL)) {
            return getFieldType(types.get(0));
          }
        }
      case FIXED:
        return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.STRING);
      case STRING:
        return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.STRING);
      case BYTES:
        return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.BYTES);
      case INT:
        return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.INT32);
      case LONG:
        return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.INT64);
      case RECORD:
        return SamzaSqlFieldType.createRowFieldType(getSchema(schema.getFields()));
      case MAP:
        return SamzaSqlFieldType.createMapFieldType(getFieldType(schema.getValueType()));
      default:
        String msg = String.format("Field Type %s is not supported", schema.getType());
        LOG.error(msg);
        throw new SamzaException(msg);
    }
  }

}
