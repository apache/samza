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

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.SamzaException;
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

  public RelDataType createType(Schema schema) {
    Schema.Type type = schema.getType();
    if (type != Schema.Type.RECORD) {
      String msg =
          String.format("System supports only RECORD as top level avro type, But the Schema's type is %s", type);
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    return convertRecord(schema);
  }

  private RelDataType convertRecord(Schema schema) {
    List<RelDataTypeField> relFields = getRelFields(schema.getFields());
    return new RelRecordType(relFields);
  }

  private List<RelDataTypeField> getRelFields(List<Schema.Field> fields) {
    List<RelDataTypeField> relFields = new ArrayList<>();

    for (Schema.Field field : fields) {
      String fieldName = field.name();
      int fieldPos = field.pos() + 1;
      RelDataType dataType = getRelDataType(field.schema());
      relFields.add(new RelDataTypeFieldImpl(fieldName, fieldPos, dataType));
    }

    return relFields;
  }

  private RelDataType getRelDataType(Schema fieldSchema) {
    switch (fieldSchema.getType()) {
      case ARRAY:
        // TODO JavaTypeFactoryImpl should convert Array into Array(ANY, ANY)
        // return new ArraySqlType(createSqlType(SqlTypeName.ANY), true);
        return createTypeWithNullability(createSqlType(SqlTypeName.ANY), true);
      case BOOLEAN:
        return createTypeWithNullability(createSqlType(SqlTypeName.BOOLEAN), true);
      case DOUBLE:
        return createTypeWithNullability(createSqlType(SqlTypeName.DOUBLE), true);
      case FLOAT:
        return createTypeWithNullability(createSqlType(SqlTypeName.FLOAT), true);
      case ENUM:
        return createTypeWithNullability(createSqlType(SqlTypeName.VARCHAR), true);
      case UNION:
        return getRelTypeFromUnionTypes(fieldSchema.getTypes());
      case FIXED:
        return createTypeWithNullability(createSqlType(SqlTypeName.VARBINARY), true);
      case STRING:
        return createTypeWithNullability(createSqlType(SqlTypeName.VARCHAR), true);
      case BYTES:
        return createTypeWithNullability(createSqlType(SqlTypeName.VARBINARY), true);
      case INT:
        return createTypeWithNullability(createSqlType(SqlTypeName.INTEGER), true);
      case LONG:
        return createTypeWithNullability(createSqlType(SqlTypeName.BIGINT), true);
      case RECORD:
        // return createTypeWithNullability(convertRecord(fieldSchema), true);
        // TODO Calcite execution engine doesn't support record type yet.
        return createTypeWithNullability(createSqlType(SqlTypeName.ANY), true);
      case MAP:
        // JavaTypeFactoryImpl converts map into Map(ANY, ANY)
        return super.createMapType(createTypeWithNullability(createSqlType(SqlTypeName.ANY), true),
            createTypeWithNullability(createSqlType(SqlTypeName.ANY), true));
      default:
        String msg = String.format("Field Type %s is not supported", fieldSchema.getType());
        LOG.error(msg);
        throw new SamzaException(msg);
    }
  }

  private RelDataType getRelTypeFromUnionTypes(List<Schema> types) {
    // Typically a nullable field's schema is configured as an union of Null and a Type.
    // This is to check whether the Union is a Nullable field
    if (types.size() == 2) {
      if (types.get(0).getType() == Schema.Type.NULL) {
        return getRelDataType(types.get(1));
      } else if ((types.get(1).getType() == Schema.Type.NULL)) {
        return getRelDataType(types.get(0));
      }
    }

    return createTypeWithNullability(createSqlType(SqlTypeName.VARCHAR), true);
  }
}
