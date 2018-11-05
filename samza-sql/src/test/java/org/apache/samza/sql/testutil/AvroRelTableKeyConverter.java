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

package org.apache.samza.sql.testutil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.samza.config.Config;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.avro.AvroRelSchemaProvider;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelTableKeyConverter;
import org.apache.samza.system.SystemStream;

import static org.apache.samza.sql.avro.AvroRelConverter.convertToGenericRecord;


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
public class AvroRelTableKeyConverter implements SamzaRelTableKeyConverter {

  protected final Config config;
  private final Schema keySchema;

  public AvroRelTableKeyConverter(SystemStream systemStream, AvroRelSchemaProvider schemaProvider, Config config) {
    this.config = config;
    this.keySchema = Schema.parse(schemaProvider.getSchema(systemStream));
  }

  /**
   * Convert the nested relational message to the output samza message.
   */
  @Override
  public Object convertToTableKeyFormat(SamzaSqlRelRecord relRecord) {
    return convertToTableKeyFormat(relRecord, this.keySchema);
  }

  protected Object convertToTableKeyFormat(SamzaSqlRelRecord relRecord, Schema payloadSchema) {
    return convertToGenericRecord(relRecord, payloadSchema);
  }
}
