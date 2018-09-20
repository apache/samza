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

package org.apache.samza.tools.avro;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.avro.AvroRelConverter;
import org.apache.samza.sql.avro.AvroRelSchemaProvider;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.system.SystemStream;


/**
 * Special form for AvroRelConverter that generates the avro schema on the output based on the
 * fields in {@link SamzaSqlRelMessage} and uses the schema to serialize the output.
 * This is useful to test out the SQL quickly when the destination system supports Avro serialized data,
 * without having to manually author the avro schemas for various SQL queries.
 */
public class AvroSchemaGenRelConverter extends AvroRelConverter {

  private final String streamName;
  private Map<String, Schema> schemas = new HashMap<>();

  public AvroSchemaGenRelConverter(SystemStream systemStream, AvroRelSchemaProvider schemaProvider, Config config) {
    super(systemStream, schemaProvider, config);
    streamName = systemStream.getStream();
  }

  @Override
  public KV<Object, Object> convertToSamzaMessage(SamzaSqlRelMessage relMessage) {
    Schema schema = computeSchema(streamName, relMessage);
    return convertToSamzaMessage(relMessage, schema);
  }

  private Schema computeSchema(String streamName, SamzaSqlRelMessage relMessage) {
    List<Schema.Field> keyFields = new ArrayList<>();
    List<String> fieldNames = relMessage.getSamzaSqlRelRecord().getFieldNames();
    List<Object> values = relMessage.getSamzaSqlRelRecord().getFieldValues();

    for (int index = 0; index < fieldNames.size(); index++) {
      if (fieldNames.get(index).equals(SamzaSqlRelMessage.KEY_NAME) || values.get(index) == null) {
        continue;
      }

      Object value = values.get(index);
      Schema avroType;
      if (value instanceof GenericData.Record) {
        avroType = ((GenericData.Record) value).getSchema();
      } else {
        avroType = ReflectData.get().getSchema(value.getClass());
      }
      keyFields.add(new Schema.Field(fieldNames.get(index), avroType, "", null));
    }

    Schema ks = Schema.createRecord(streamName, "", streamName + "_namespace", false);
    ks.setFields(keyFields);
    String schemaStr = ks.toString();
    Schema schema;
    // See whether we have a schema object corresponding to the schemaValue and reuse it.
    // CachedSchemaRegistryClient doesn't like if we recreate schema objects.
    if (schemas.containsKey(schemaStr)) {
      schema = schemas.get(schemaStr);
    } else {
      schema = Schema.parse(schemaStr);
      schemas.put(schemaStr, schema);
    }

    return schema;
  }
}
