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

package org.apache.samza.sql.avro;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.schema.SqlFieldSchema;
import org.apache.samza.sql.schema.SqlSchema;
import org.apache.samza.sql.schema.SqlSchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory that creates {@link SqlSchema} from the Avro Schema. This is used by the
 * {@link AvroRelConverter} to convert Avro schema to Samza Sql schema.
 */
public class AvroTypeFactoryImpl extends SqlTypeFactoryImpl {

  private static final Logger LOG = LoggerFactory.getLogger(AvroTypeFactoryImpl.class);

  public AvroTypeFactoryImpl() {
    super(RelDataTypeSystem.DEFAULT);
  }

  public SqlSchema createType(Schema schema) {
    Schema.Type type = schema.getType();
    if (type != Schema.Type.RECORD) {
      String msg =
          String.format("System supports only RECORD as top level avro type, But the Schema's type is %s", type);
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    return convertSchema(schema.getFields());
  }

  private SqlSchema convertSchema(List<Schema.Field> fields) {

    SqlSchemaBuilder schemaBuilder = SqlSchemaBuilder.builder();
    for (Schema.Field field : fields) {
      boolean isOptional = (field.defaultValue() != null);
      SqlFieldSchema fieldSchema = convertField(field.schema(), false, isOptional);
      schemaBuilder.addField(field.name(), fieldSchema);
    }

    return schemaBuilder.build();
  }

  private SqlFieldSchema convertField(Schema fieldSchema) {
    return convertField(fieldSchema, false, false);
  }

  private SqlFieldSchema convertField(Schema fieldSchema, boolean isNullable, boolean isOptional) {
    switch (fieldSchema.getType()) {
      case ARRAY:
        SqlFieldSchema elementSchema = convertField(fieldSchema.getElementType());
        return SqlFieldSchema.createArraySchema(elementSchema, isNullable, isOptional);
      case BOOLEAN:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.BOOLEAN, isNullable, isOptional);
      case DOUBLE:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.DOUBLE, isNullable, isOptional);
      case FLOAT:
        // Avro FLOAT is 4 bytes which maps to Sql REAL. Sql FLOAT is 8-bytes
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.REAL, isNullable, isOptional);
      case ENUM:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.STRING, isNullable, isOptional);
      case UNION:
        return getSqlTypeFromUnionTypes(fieldSchema.getTypes(), isNullable, isOptional);
      case FIXED:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.BYTES, isNullable, isOptional);
      case STRING:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.STRING, isNullable, isOptional);
      case BYTES:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.BYTES, isNullable, isOptional);
      case INT:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.INT32, isNullable, isOptional);
      case LONG:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.INT64, isNullable, isOptional);
      case RECORD:
        SqlSchema rowSchema = convertSchema(fieldSchema.getFields());
        return SqlFieldSchema.createRowFieldSchema(rowSchema, isNullable, isOptional);
      case MAP:
        // Can the value type be nullable and have default values ? Guess not!
        SqlFieldSchema valueType = convertField(fieldSchema.getValueType(), false, false);
        return SqlFieldSchema.createMapSchema(valueType, isNullable, isOptional);
      default:
        String msg = String.format("Field Type %s is not supported", fieldSchema.getType());
        LOG.error(msg);
        throw new SamzaException(msg);
    }
  }

  private SqlFieldSchema getSqlTypeFromUnionTypes(List<Schema> types, boolean isNullable, boolean isOptional) {
    // Typically a nullable field's schema is configured as an union of Null and a Type.
    // This is to check whether the Union is a Nullable field
    if (types.size() == 2) {
      if (types.get(0).getType() == Schema.Type.NULL) {
        return convertField(types.get(1), true, isOptional);
      } else if ((types.get(1).getType() == Schema.Type.NULL)) {
        return convertField(types.get(0), true, isOptional);
      }
    }

    return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.ANY, isNullable, isOptional);
  }
}
