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

package org.apache.samza.sql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.avro.AvroRelConverter;
import org.apache.samza.sql.avro.AvroRelSchemaProvider;
import org.apache.samza.sql.avro.ConfigBasedAvroRelSchemaProviderFactory;
import org.apache.samza.sql.avro.schemas.AddressRecord;
import org.apache.samza.sql.avro.schemas.Profile;
import org.apache.samza.sql.avro.schemas.StreetNumRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.serializers.SamzaSqlRelMessageSerdeFactory;
import org.apache.samza.system.SystemStream;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.sql.serializers.SamzaSqlRelMessageSerdeFactory.SamzaSqlRelMessageSerde;


public class TestSamzaSqlRelMessageSerde {

  private List<Object> values = Arrays.asList("value1", 1, null);
  private List<String> names = Arrays.asList("field1", "field2", "field3");

  @Test
  public void testWithDifferentFields() {
    SamzaSqlRelMessage message = new SamzaSqlRelMessage(names, values);
    SamzaSqlRelMessageSerde serde =
        (SamzaSqlRelMessageSerde) new SamzaSqlRelMessageSerdeFactory().getSerde(null, null);
    SamzaSqlRelMessage resultMsg = serde.fromBytes(serde.toBytes(message));
    Assert.assertEquals(names, resultMsg.getSamzaSqlRelRecord().getFieldNames());
    Assert.assertEquals(values, resultMsg.getSamzaSqlRelRecord().getFieldValues());
  }

  @Test
  public void testNestedRecordConversion() {
    Map<String, String> props = new HashMap<>();
    SystemStream ss1 = new SystemStream("test", "nestedRecord");
    props.put(
        String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA, ss1.getSystem(), ss1.getStream()),
        Profile.SCHEMA$.toString());
    ConfigBasedAvroRelSchemaProviderFactory factory = new ConfigBasedAvroRelSchemaProviderFactory();
    AvroRelSchemaProvider nestedRecordSchemaProvider = (AvroRelSchemaProvider) factory.create(ss1, new MapConfig(props));
    AvroRelConverter nestedRecordAvroRelConverter = new AvroRelConverter(ss1, nestedRecordSchemaProvider, new MapConfig());

    Pair<SamzaSqlRelMessage, GenericData.Record> messageRecordPair =
        createNestedSamzaSqlRelMessage(nestedRecordAvroRelConverter);
    SamzaSqlRelMessageSerde serde =
        (SamzaSqlRelMessageSerde) new SamzaSqlRelMessageSerdeFactory().getSerde(null, null);
    SamzaSqlRelMessage resultMsg = serde.fromBytes(serde.toBytes(messageRecordPair.getKey()));
    KV<Object, Object> samzaMessage = nestedRecordAvroRelConverter.convertToSamzaMessage(resultMsg);
    GenericRecord recordPostConversion = (GenericRecord) samzaMessage.getValue();

    for (Schema.Field field : Profile.SCHEMA$.getFields()) {
      // equals() on GenericRecord does the nested record equality check as well.
      Assert.assertEquals(messageRecordPair.getValue().get(field.name()), recordPostConversion.get(field.name()));
    }
  }

  public static Pair<SamzaSqlRelMessage, GenericData.Record> createNestedSamzaSqlRelMessage(
      AvroRelConverter nestedRecordAvroRelConverter) {
    GenericData.Record record = new GenericData.Record(Profile.SCHEMA$);
    record.put("id", 1);
    record.put("name", "name1");
    record.put("companyId", 0);
    GenericData.Record addressRecord = new GenericData.Record(AddressRecord.SCHEMA$);
    addressRecord.put("zip", 90000);
    record.put("address", addressRecord);
    GenericData.Record streetNumRecord = new GenericData.Record(StreetNumRecord.SCHEMA$);
    streetNumRecord.put("number", 1200);
    addressRecord.put("streetnum", streetNumRecord);
    return Pair.of(nestedRecordAvroRelConverter.convertToRelMessage(new KV<>("key", record)), record);
  }

}