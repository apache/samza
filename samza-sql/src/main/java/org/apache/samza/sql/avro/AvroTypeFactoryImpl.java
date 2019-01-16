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
      SqlFieldSchema fieldSchema = convertField(field.schema());
      schemaBuilder.addField(field.name(), fieldSchema);
    }

    return schemaBuilder.build();
  }

  private SqlFieldSchema convertField(Schema fieldSchema) {
    switch (fieldSchema.getType()) {
      case ARRAY:
        SqlFieldSchema elementSchema = convertField(fieldSchema.getElementType());
        return SqlFieldSchema.createArrayFieldType(elementSchema);
      case BOOLEAN:
        return SqlFieldSchema.createPrimitiveFieldType(SamzaSqlFieldType.BOOLEAN);
      case DOUBLE:
        return SqlFieldSchema.createPrimitiveFieldType(SamzaSqlFieldType.DOUBLE);
      case FLOAT:
        return SqlFieldSchema.createPrimitiveFieldType(SamzaSqlFieldType.FLOAT);
      case ENUM:
        return SqlFieldSchema.createPrimitiveFieldType(SamzaSqlFieldType.STRING);
      case UNION:
        return getSqlTypeFromUnionTypes(fieldSchema.getTypes());
      case FIXED:
        return SqlFieldSchema.createPrimitiveFieldType(SamzaSqlFieldType.BYTES);
      case STRING:
        return SqlFieldSchema.createPrimitiveFieldType(SamzaSqlFieldType.STRING);
      case BYTES:
        return SqlFieldSchema.createPrimitiveFieldType(SamzaSqlFieldType.BYTES);
      case INT:
        return SqlFieldSchema.createPrimitiveFieldType(SamzaSqlFieldType.INT32);
      case LONG:
        return SqlFieldSchema.createPrimitiveFieldType(SamzaSqlFieldType.INT64);
      case RECORD:
        SqlSchema rowSchema = convertSchema(fieldSchema.getFields());
        return SqlFieldSchema.createRowFieldType(rowSchema);
      case MAP:
        SqlFieldSchema valueType = convertField(fieldSchema.getValueType());
        return SqlFieldSchema.createMapFieldType(valueType);
      default:
        String msg = String.format("Field Type %s is not supported", fieldSchema.getType());
        LOG.error(msg);
        throw new SamzaException(msg);
    }
  }

  private SqlFieldSchema getSqlTypeFromUnionTypes(List<Schema> types) {
    // Typically a nullable field's schema is configured as an union of Null and a Type.
    // This is to check whether the Union is a Nullable field
    if (types.size() == 2) {
      if (types.get(0).getType() == Schema.Type.NULL) {
        return convertField(types.get(1));
      } else if ((types.get(1).getType() == Schema.Type.NULL)) {
        return convertField(types.get(0));
      }
    }

    return SqlFieldSchema.createPrimitiveFieldType(SamzaSqlFieldType.ANY);
  }
}
