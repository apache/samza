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
 * Factory that creates the Calcite relational types from the Avro Schema. This is used by the
 * AvroRelConverter to convert the Avro schema to calcite relational schema.
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
      // Consider any field with default value as nullable. Is it the right assumption ?
      boolean isNullable = field.defaultValue() != null;
      SqlFieldSchema fieldSchema = convertField(field.schema(), isNullable);
      schemaBuilder.addField(field.name(), fieldSchema);
    }

    return schemaBuilder.build();
  }

  private SqlFieldSchema convertField(Schema fieldSchema) {
    return convertField(fieldSchema, false);
  }

  private SqlFieldSchema convertField(Schema fieldSchema, boolean isNullable) {
    switch (fieldSchema.getType()) {
      case ARRAY:
        SqlFieldSchema elementSchema = convertField(fieldSchema.getElementType());
        return SqlFieldSchema.createArraySchema(elementSchema, isNullable);
      case BOOLEAN:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.BOOLEAN, isNullable);
      case DOUBLE:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.DOUBLE, isNullable);
      case FLOAT:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.FLOAT, isNullable);
      case ENUM:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.STRING, isNullable);
      case UNION:
        return getSqlTypeFromUnionTypes(fieldSchema.getTypes(), isNullable);
      case FIXED:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.BYTES, isNullable);
      case STRING:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.STRING, isNullable);
      case BYTES:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.BYTES, isNullable);
      case INT:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.INT32, isNullable);
      case LONG:
        return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.INT64, isNullable);
      case RECORD:
        SqlSchema rowSchema = convertSchema(fieldSchema.getFields());
        return SqlFieldSchema.createRowFieldSchema(rowSchema, isNullable);
      case MAP:
        SqlFieldSchema valueType = convertField(fieldSchema.getValueType(), isNullable);
        return SqlFieldSchema.createMapSchema(valueType, isNullable);
      default:
        String msg = String.format("Field Type %s is not supported", fieldSchema.getType());
        LOG.error(msg);
        throw new SamzaException(msg);
    }
  }

  private SqlFieldSchema getSqlTypeFromUnionTypes(List<Schema> types, boolean isNullable) {
    // Typically a nullable field's schema is configured as an union of Null and a Type.
    // This is to check whether the Union is a Nullable field
    if (types.size() == 2) {
      if (types.get(0).getType() == Schema.Type.NULL) {
        return convertField(types.get(1), true);
      } else if ((types.get(1).getType() == Schema.Type.NULL)) {
        return convertField(types.get(0), true);
      }
    }

    return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.ANY, isNullable);
  }
}
