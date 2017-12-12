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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
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
 *     The value part of the samza message is expected to be {@link IndexedRecord}, All the fields in the IndexedRecord form
 *     the corresponding fields of the relational message.
 *
 * Conversion from Relational to Samza Message :
 *     This converts the Samza relational message into Avro {@link GenericRecord}.
 *     All the fields of the relational message is become fields of the Avro GenericRecord except of the field with name
 *     {@link SamzaSqlRelMessage#KEY_NAME}. This special field becomes the Key in the output Samza message.
 */
public class AvroRelConverter implements SamzaRelConverter {

  protected final Config config;
  private final Schema avroSchema;
  private final RelDataType relationalSchema;

  /**
   * Class that converts the avro field to their corresponding relational fields
   * Array fields are converted from Avro {@link org.apache.avro.generic.GenericData.Array} to {@link ArrayList}
   */
  public enum AvroToRelObjConverter {

    /**
     * If the relational field type is ArraySqlType, We expect the avro field to be of type either
     * {@link GenericData.Array} or {@link List} which then is converted to Rel field of type {@link ArrayList}
     */
    ArraySqlType {
      @Override
      Object convert(Object avroObj) {
        ArrayList<Object> retVal = new ArrayList<>();
        if (avroObj != null) {
          if (avroObj instanceof GenericData.Array) {
            retVal.addAll(((GenericData.Array) avroObj));
          } else if (avroObj instanceof List) {
            retVal.addAll((List) avroObj);
          }
        }

        return retVal;
      }
    },

    /**
     * If the relational field type is MapSqlType, We expect the avro field to be of type
     * {@link Map}
     */
    MapSqlType {
      @Override
      Object convert(Object obj) {
        Map<String, Object> retVal = new HashMap<>();
        if (obj != null) {
          retVal.putAll((Map<String, ?>) obj);
        }
        return retVal;
      }
    },

    /**
     * If the relational field type is RelRecordType, The field is considered an object
     * and moved to rel field without any translation.
     */
    RelRecordType {
      @Override
      Object convert(Object obj) {
        return obj;
      }
    },

    /**
     * If the relational field type is BasicSqlType, The field is moved to rel field without any translation.
     */
    BasicSqlType {
      @Override
      Object convert(Object obj) {
        return obj;
      }
    };

    abstract Object convert(Object obj);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AvroRelConverter.class);

  private final Schema arraySchema = Schema.parse(
      "{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Object\",\"namespace\":\"java.lang\",\"fields\":[]},\"java-class\":\"java.util.List\"}");
  private final Schema mapSchema = Schema.parse(
      "{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"Object\",\"namespace\":\"java.lang\",\"fields\":[]}}");


  public AvroRelConverter(SystemStream systemStream, AvroRelSchemaProvider schemaProvider, Config config) {
    this.config = config;
    this.relationalSchema = schemaProvider.getRelationalSchema();
    this.avroSchema = Schema.parse(schemaProvider.getSchema(systemStream));
  }

  @Override
  public SamzaSqlRelMessage convertToRelMessage(KV<Object, Object> samzaMessage) {
    List<Object> values = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    Object value = samzaMessage.getValue();
    if (value instanceof IndexedRecord) {
      IndexedRecord record = (IndexedRecord) value;
      fieldNames.addAll(relationalSchema.getFieldNames());
      values.addAll(relationalSchema.getFieldList()
          .stream()
          .map(x -> getRelField(x.getType(), record.get(this.avroSchema.getField(x.getName()).pos())))
          .collect(Collectors.toList()));
    } else if (value == null) {
      fieldNames.addAll(relationalSchema.getFieldNames());
      IntStream.range(0, fieldNames.size()).forEach(x -> values.add(null));
    } else {
      String msg = "Avro message converter doesn't support messages of type " + value.getClass();
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    return new SamzaSqlRelMessage(samzaMessage.getKey(), fieldNames, values);
  }

  @Override
  public KV<Object, Object> convertToSamzaMessage(SamzaSqlRelMessage relMessage) {
    return convertToSamzaMessage(relMessage, this.avroSchema);
  }

  protected KV<Object, Object> convertToSamzaMessage(SamzaSqlRelMessage relMessage, Schema avroSchema) {
    GenericRecord record = new GenericData.Record(avroSchema);
    List<String> fieldNames = relMessage.getFieldNames();
    List<Object> values = relMessage.getFieldValues();
    for (int index = 0; index < fieldNames.size(); index++) {
      record.put(fieldNames.get(index), values.get(index));
    }

    return new KV<>(relMessage.getKey(), record);
  }

  private Object getRelField(RelDataType relType, Object avroObj) {
    return AvroToRelObjConverter.valueOf(relType.getClass().getSimpleName()).convert(avroObj);
  }
}
