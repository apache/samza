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
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.Validate;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.system.SystemStream;


/**
 * Special form of {@link AvroRelConverter} that handles keys in avro format.
 */
public class AvroRelConverterWithKeyRecord extends AvroRelConverter {

  private final Schema keySchema;

  public AvroRelConverterWithKeyRecord(SystemStream systemStream, AvroRelSchemaProvider schemaProvider, Config config) {
    super(systemStream, schemaProvider, config);
    this.keySchema = schemaProvider.getKeySchema(systemStream) == null ?
        null : Schema.parse(schemaProvider.getKeySchema(systemStream));
    Validate.isTrue(this.keySchema != null, "Could not obtain schema for the key.");
  }

  @Override
  public SamzaSqlRelMessage convertToRelMessage(KV<Object, Object> samzaMessage) {
    Object key = samzaMessage.getKey();
    Validate.isTrue(key instanceof IndexedRecord);

    List<String> keyFieldNames = new ArrayList<>();
    List<Object> keyFieldValues = new ArrayList<>();
    fetchFieldNamesAndValuesFromIndexedRecord((IndexedRecord) key, keyFieldNames, keyFieldValues, keySchema);
    key = new SamzaSqlRelRecord(keyFieldNames, keyFieldValues);
    return super.convertToRelMessage(new KV<>(key, samzaMessage.getValue()));
  }

  @Override
  public KV<Object, Object> convertToSamzaMessage(SamzaSqlRelMessage relMessage) {
    KV<Object, Object> samzaMessage = super.convertToSamzaMessage(relMessage);
    return new KV<>(convertKey(relMessage.getKey(), this.keySchema), samzaMessage.getValue());
  }

  protected KV<Object, Object> convertToSamzaMessage(SamzaSqlRelMessage relMessage, Schema keySchema,
      Schema payloadSchema) {
    KV<Object, Object> samzaMessage = super.convertToSamzaMessage(relMessage, payloadSchema);
    return new KV<>(convertKey(relMessage.getKey(), keySchema), samzaMessage.getValue());
  }

  private Object convertKey(Object key, Schema keySchema) {
    Validate.isTrue(key instanceof SamzaSqlRelRecord);
    return convertToGenericRecord((SamzaSqlRelRecord) key, keySchema);
  }

}
