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
 *
 * Below is some background on Nullability and Optionality of fields in Avro.
 *
 * Nullable fields in avro are of type union with one of the values being null.
 * Fields with default values are optional ONLY while reading. Default values are
 * meant for schema evolution where reader schema is different from writer schema.
 * Please note here that even the fields with default values MUST be specified at
 * the time of serialization.
 *
 * Behavior on the consumption side:
 * While reading, the fields that are not written will be filled with default values
 * that are specified in the reader schema. Please note that this happens only when
 * the field with default value is not present in writer schema but is present in
 * reader schema.
 *
 * Behavior on the producer side:
 * All non-nullable avro fields should have values including fields with default values.
 * Nullable fields need not be explicitly set as they are set to null by
 * {@link org.apache.avro.generic.GenericDatumWriter} during serialization.
 *
 * So, all nullable fields are optional fields on the producer side in Avro.
 * {@link AvroTypeFactoryImpl} reflects the state of fields on the producer side.
 *
 * Note: There could be cases where the producer might embed some fields, in which case
 * a non-nullable field is optional in Samza Sql.
 */
public class AvroTypeFactoryImpl extends SqlTypeFactoryImpl {

  private static final Logger LOG = LoggerFactory.getLogger(AvroTypeFactoryImpl.class);

  public AvroTypeFactoryImpl() {
    super(RelDataTypeSystem.DEFAULT);
  }

  public SqlSchema createType(Schema schema) {
    validateTopLevelAvroType(schema);
    return convertSchema(schema.getFields(), true);
  }

  /**
   * Given a schema field, determine if it is an optional field. There could be cases where a field
   * is considered as optional even if it is marked as non-nullable in the schema. The producer could be filling in this
   * field and hence need not be specified in the query and hence is optional. Typically, such fields are
   * the top level fields in the schema.
   * @param field schema field
   * @param isTopLevelField if it is top level field in the schema
   * @return if the field is optional
   */
  protected boolean isOptional(Schema.Field field, boolean isTopLevelField) {
    return false;
  }

  private void validateTopLevelAvroType(Schema schema) {
    Schema.Type type = schema.getType();
    if (type != Schema.Type.RECORD) {
      String msg =
          String.format("Samza Sql supports only RECORD as top level avro type, But the Schema's type is %s", type);
      LOG.error(msg);
      throw new SamzaException(msg);
    }
  }

  private SqlSchema convertSchema(List<Schema.Field> fields, boolean isTopLevelField) {
    SqlSchemaBuilder schemaBuilder = SqlSchemaBuilder.builder();
    for (Schema.Field field : fields) {
      SqlFieldSchema fieldSchema = convertField(field.schema(), false, isOptional(field, isTopLevelField));
      schemaBuilder.addField(field.name(), fieldSchema);
    }

    return schemaBuilder.build();
  }

  private SqlFieldSchema convertField(Schema fieldSchema, boolean isNullable, boolean isOptional) {
    switch (fieldSchema.getType()) {
      case ARRAY:
        SqlFieldSchema elementSchema = convertField(fieldSchema.getElementType(), false, false);
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
        SqlSchema rowSchema = convertSchema(fieldSchema.getFields(), false);
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
    if (types.size() == 2) {
      if (types.get(0).getType() == Schema.Type.NULL) {
        return convertField(types.get(1), true, true);
      } else if (types.get(1).getType() == Schema.Type.NULL) {
        return convertField(types.get(0), true, true);
      }
    } else if (types.size() > 2) {
      // Union field with more than 2 Types is considered nullable and optional only if one of the Types is NULL.
      boolean isNullTypeFound = types.stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
      if (isNullTypeFound) {
        isNullable = isNullTypeFound;
        isOptional = isNullTypeFound;
      }
    }

    // If we reach here, it means that the union has two or more non-null types. Mark the type for such fields
    // as ANY.
    return SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.ANY, isNullable, isOptional);
  }
}
