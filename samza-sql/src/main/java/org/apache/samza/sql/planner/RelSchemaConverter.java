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

package org.apache.samza.sql.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.schema.SqlFieldSchema;
import org.apache.samza.sql.schema.SqlSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that converts the {@link SqlSchema} to Calcite relational schema
 */
public class RelSchemaConverter extends SqlTypeFactoryImpl {

  private static final Logger LOG = LoggerFactory.getLogger(RelSchemaConverter.class);

  public RelSchemaConverter() {
    super(RelDataTypeSystem.DEFAULT);
  }

  public RelDataType convertToRelSchema(SqlSchema sqlSchema) {
    return convertRecordType(sqlSchema);
  }

  private RelDataType convertRecordType(SqlSchema schema) {
    List<RelDataTypeField> relFields = getRelFields(schema.getFields());
    return new RelRecordType(relFields);
  }

  private List<RelDataTypeField> getRelFields(List<SqlSchema.SqlField> fields) {
    List<RelDataTypeField> relFields = new ArrayList<>();

    for (SqlSchema.SqlField field : fields) {
      String fieldName = field.getFieldName();
      int fieldPos = field.getPosition();
      RelDataType dataType = getRelDataType(field.getFieldSchema());
      relFields.add(new RelDataTypeFieldImpl(fieldName, fieldPos, dataType));
    }

    return relFields;
  }

  private RelDataType getRelDataType(SqlFieldSchema fieldSchema) {
    switch (fieldSchema.getFieldType()) {
      case ARRAY:
        RelDataType elementType = getRelDataType(fieldSchema.getElementSchema());
        return new ArraySqlType(elementType, true);
      case BOOLEAN:
        return createTypeWithNullability(createSqlType(SqlTypeName.BOOLEAN), true);
      case DOUBLE:
        return createTypeWithNullability(createSqlType(SqlTypeName.DOUBLE), true);
      case FLOAT:
        return createTypeWithNullability(createSqlType(SqlTypeName.FLOAT), true);
      case STRING:
        return createTypeWithNullability(createSqlType(SqlTypeName.VARCHAR), true);
      case BYTES:
        return createTypeWithNullability(createSqlType(SqlTypeName.VARBINARY), true);
      case INT16:
      case INT32:
        return createTypeWithNullability(createSqlType(SqlTypeName.INTEGER), true);
      case INT64:
        return createTypeWithNullability(createSqlType(SqlTypeName.BIGINT), true);
      case ROW:
      case ANY:
        // TODO Calcite execution engine doesn't support record type yet.
        return createTypeWithNullability(createSqlType(SqlTypeName.ANY), true);
      case MAP:
        RelDataType valueType = getRelDataType(fieldSchema.getValueScehma());
        return super.createMapType(createTypeWithNullability(createSqlType(SqlTypeName.VARCHAR), true),
            createTypeWithNullability(valueType, true));
      default:
        String msg = String.format("Field Type %s is not supported", fieldSchema.getFieldType());
        LOG.error(msg);
        throw new SamzaException(msg);
    }
  }
}
