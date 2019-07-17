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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.commons.lang3.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.data.SamzaSqlRelMsgMetadata;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class converts a Samza Avro messages to Relational messages and vice versa.
 * This supports Samza messages where Key is a string and Value is an avro record.
 *
 * Conversion from Samza to Relational Message :
 *     The key part of the samza message is represented as a special column {@link SamzaSqlRelMessage#KEY_NAME}
 *     in relational message.
 *
 *     The value part of the samza message is expected to be {@link IndexedRecord}, All the fields in the IndexedRecord
 *     form the corresponding fields of the relational message.
 *
 * Conversion from Relational to Samza Message :
 *     This converts the Samza relational message into Avro {@link GenericRecord}.
 *     All the fields of the relational message become fields of the Avro GenericRecord except the field with name
 *     {@link SamzaSqlRelMessage#KEY_NAME}. This special field becomes the Key in the output Samza message.
 */
public class AvroRelConverter implements SamzaRelConverter {

  protected final Config config;
  private final Schema payloadSchema;

  private static final Logger LOG = LoggerFactory.getLogger(AvroRelConverter.class);

  public AvroRelConverter(SystemStream systemStream, AvroRelSchemaProvider schemaProvider, Config config) {
    this.config = config;
    String schema = schemaProvider.getSchema(systemStream);
    this.payloadSchema = schema == null ? null : Schema.parse(schema);
  }

  /**
   * Converts the nested avro object in SamzaMessage to relational message corresponding to
   * the tableName with relational schema.
   */
  @Override
  public SamzaSqlRelMessage convertToRelMessage(KV<Object, Object> samzaMessage) {
    List<String> payloadFieldNames = new ArrayList<>();
    List<Object> payloadFieldValues = new ArrayList<>();
    Object value = samzaMessage.getValue();
    if (value instanceof IndexedRecord) {
      fetchFieldNamesAndValuesFromIndexedRecord((IndexedRecord) value, payloadFieldNames, payloadFieldValues,
          payloadSchema);
    } else if (value == null) {
      // If the payload is null, set each record value as null
      payloadFieldNames.addAll(payloadSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()));
      IntStream.range(0, payloadFieldNames.size()).forEach(x -> payloadFieldValues.add(null));
    } else {
      String msg = "Avro message converter doesn't support messages of type " + value.getClass();
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    return new SamzaSqlRelMessage(samzaMessage.getKey(), payloadFieldNames, payloadFieldValues,
        new SamzaSqlRelMsgMetadata("", "", ""));
  }

  /**
   * Create a SamzaSqlRelMessage for the specified key and Avro record using the schema from the Avro record.
   *
   */
  public static SamzaSqlRelMessage convertToRelMessage(Object key, IndexedRecord record, Schema schema) {
    List<String> payloadFieldNames = new ArrayList<>();
    List<Object> payloadFieldValues = new ArrayList<>();
    fetchFieldNamesAndValuesFromIndexedRecord(record, payloadFieldNames, payloadFieldValues, schema);
    return new SamzaSqlRelMessage(key, payloadFieldNames, payloadFieldValues,
        new SamzaSqlRelMsgMetadata("", "", ""));
  }

  public static void fetchFieldNamesAndValuesFromIndexedRecord(IndexedRecord record, List<String> fieldNames,
      List<Object> fieldValues, Schema cachedSchema) {
    // Please note that record schema and cached schema could be different due to schema evolution.
    // Always represent record schema in the form of cached schema. This approach has the side-effect
    // of dropping the newly added fields in the scenarios where the record schema has newer version
    // than the cached schema. [TODO: SAMZA-1679]
    Schema recordSchema = record.getSchema();
    fieldNames.addAll(cachedSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()));
    fieldValues.addAll(fieldNames.stream()
        .map(f -> convertToJavaObject(
            recordSchema.getField(f) != null ? record.get(recordSchema.getField(f).pos()) : null,
            getNonNullUnionSchema(cachedSchema.getField(f).schema()))) // get schema from cachedSchema
        .collect(Collectors.toList()));
  }

  private static SamzaSqlRelRecord convertToRelRecord(IndexedRecord avroRecord) {
    List<Object> fieldValues = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    if (avroRecord != null) {
      fieldNames.addAll(
          avroRecord.getSchema().getFields().stream().map(Schema.Field::name).collect(Collectors.toList()));
      fieldValues.addAll(avroRecord.getSchema()
          .getFields()
          .stream()
          .map(f -> convertToJavaObject(avroRecord.get(avroRecord.getSchema().getField(f.name()).pos()),
              getNonNullUnionSchema(avroRecord.getSchema().getField(f.name()).schema())))
          .collect(Collectors.toList()));
    } else {
      String msg = "Avro Record is null";
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    return new SamzaSqlRelRecord(fieldNames, fieldValues);
  }

  /**
   * Convert the nested relational message to the output samza message.
   */
  @Override
  public KV<Object, Object> convertToSamzaMessage(SamzaSqlRelMessage relMessage) {
    return convertToSamzaMessage(relMessage, this.payloadSchema);
  }

  protected KV<Object, Object> convertToSamzaMessage(SamzaSqlRelMessage relMessage, Schema payloadSchema) {
    return new KV<>(relMessage.getKey(), convertToGenericRecord(relMessage.getSamzaSqlRelRecord(), payloadSchema));
  }

  private static GenericRecord convertToGenericRecord(SamzaSqlRelRecord relRecord, Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
    List<String> fieldNames = relRecord.getFieldNames();
    List<Object> values = relRecord.getFieldValues();
    for (int index = 0; index < fieldNames.size(); index++) {
      if (!fieldNames.get(index).equalsIgnoreCase(SamzaSqlRelMessage.KEY_NAME)) {
        String fieldName = fieldNames.get(index);
        /**
         * It is possible that the destination Avro schema doesn't have all the fields that are projected from the
         * SQL. This is especially possible in SQL statements like
         *        insert into kafka.outputTopic select id, company from profile
         * where company is an avro record in itself whose schema can evolve. When this happens we will end up with
         * fields in the SamzaSQLRelRecord for company field which doesn't have equivalent fields in the outputTopic's schema
         * for company. To support this scenario where the input schemas and output schemas can evolve in their own cadence,
         * We ignore the fields which doesn't have corresponding schema in the output topic.
         */
        if (schema.getField(fieldName) == null) {
          LOG.debug("Schema with Name {} and Namespace {} doesn't contain the fieldName {}, Skipping it.",
              schema.getName(), schema.getNamespace(), fieldName);
          continue;
        }
        Object relObj = values.get(index);
        Schema fieldSchema = schema.getField(fieldName).schema();
        record.put(fieldName, convertToAvroObject(relObj, getNonNullUnionSchema(fieldSchema)));
      }
    }

    return record;
  }

  public static Object convertToAvroObject(Object relObj, Schema schema) {
    if (relObj == null) {
      return null;
    }
    switch (schema.getType()) {
      case RECORD:
        return convertToGenericRecord((SamzaSqlRelRecord) relObj, getNonNullUnionSchema(schema));
      case ARRAY:
        List<Object> avroList = ((List<Object>) relObj).stream()
            .map(o -> convertToAvroObject(o, getNonNullUnionSchema(schema).getElementType()))
            .collect(Collectors.toList());
        return avroList;
      case MAP:
        return ((Map<String, ?>) relObj).entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> convertToAvroObject(e.getValue(), getNonNullUnionSchema(schema).getValueType())));
      case UNION:
        for (Schema unionSchema : schema.getTypes()) {
          if (isSchemaCompatibleWithRelObj(relObj, unionSchema)) {
            return convertToAvroObject(relObj, unionSchema);
          }
        }
        return null;
      case ENUM:
        return new GenericData.EnumSymbol(schema, (String) relObj);
      case FIXED:
        return new GenericData.Fixed(schema, ((ByteString) relObj).getBytes());
      case BYTES:
        return ByteBuffer.wrap(((ByteString) relObj).getBytes());
      default:
        return relObj;
    }
  }

  // Not doing any validations of data types with Avro schema considering the resource cost per message.
  // Casting would fail if the data types are not in sync with the schema.
  public static Object convertToJavaObject(Object avroObj, Schema schema) {
    if (avroObj == null) {
      return null;
    }
    switch (schema.getType()) {
      case RECORD:
        return convertToRelRecord((IndexedRecord) avroObj);
      case ARRAY: {
        ArrayList<Object> retVal = new ArrayList<>();
        List<Object> avroArray;
        if (avroObj instanceof GenericData.Array) {
          avroArray = (GenericData.Array) avroObj;
        } else if (avroObj instanceof List) {
          avroArray = (List) avroObj;
        } else {
          throw new SamzaException("Unsupported array type " + avroObj.getClass().getSimpleName());
        }

        retVal.addAll(avroArray.stream()
            .map(v -> convertToJavaObject(v, getNonNullUnionSchema(schema).getElementType()))
            .collect(Collectors.toList()));
        return retVal;
      }
      case MAP: {
        Map<String, Object> retVal = new HashMap<>();
        retVal.putAll(((Map<String, ?>) avroObj).entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> convertToJavaObject(e.getValue(), getNonNullUnionSchema(schema).getValueType()))));
        return retVal;
      }
      case UNION:
        for (Schema unionSchema : schema.getTypes()) {
          if (isSchemaCompatibleWithAvroObj(avroObj, unionSchema)) {
            return convertToJavaObject(avroObj, unionSchema);
          }
        }
        return null;
      case ENUM:
        return avroObj.toString();
      case FIXED:
        GenericData.Fixed fixed = (GenericData.Fixed) avroObj;
        return new ByteString(fixed.bytes());
      case BYTES:
        return new ByteString(((ByteBuffer) avroObj).array());
      case FLOAT:
        // Convert Float to Double similar to how JavaTypeFactoryImpl represents Float type
        return Double.parseDouble(Float.toString((Float) avroObj));

      default:
        return avroObj;
    }
  }

  private static boolean isSchemaCompatibleWithRelObj(Object relObj, Schema unionSchema) {
    Validate.notNull(unionSchema, "Schema cannot be null");
    if (unionSchema.getType() == Schema.Type.NULL) {
      return relObj == null;
    }

    switch (unionSchema.getType()) {
      case RECORD:
        return relObj instanceof SamzaSqlRelRecord;
      case ARRAY:
        return relObj instanceof List;
      case MAP:
        return relObj instanceof Map;
      case FIXED:
        return relObj instanceof ByteString;
      case BYTES:
        return relObj instanceof ByteString;
      case FLOAT:
        return relObj instanceof Float || relObj instanceof Double;
      default:
        return true;
    }
  }

  private static boolean isSchemaCompatibleWithAvroObj(Object avroObj, Schema unionSchema) {
    Validate.notNull(unionSchema, "Schema cannot be null");
    if (unionSchema.getType() == Schema.Type.NULL) {
      return avroObj == null;
    }
    switch (unionSchema.getType()) {
      case RECORD:
        return avroObj instanceof IndexedRecord;
      case ARRAY:
        return avroObj instanceof GenericData.Array || avroObj instanceof List;
      case MAP:
        return avroObj instanceof Map;
      case FIXED:
        return avroObj instanceof GenericData.Fixed;
      case BYTES:
        return avroObj instanceof ByteBuffer;
      case FLOAT:
        return avroObj instanceof Float;
      default:
        return true;
    }
  }

  // Two non-nullable types in a union is not yet supported.
  public static Schema getNonNullUnionSchema(Schema schema) {
    if (schema.getType().equals(Schema.Type.UNION)) {
      List<Schema> types = schema.getTypes();
      // Typically a nullable field's schema is configured as an union of Null and a Type.
      // This is to check whether the Union is a Nullable field
      if (types.size() == 2) {
        if (types.get(0).getType() == Schema.Type.NULL) {
          return types.get(1);
        } else if ((types.get(1).getType() == Schema.Type.NULL)) {
          return types.get(0);
        }
      }
    }

    return schema;
  }
}
