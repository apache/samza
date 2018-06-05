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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.avro.AvroRelConverter;
import org.apache.samza.sql.avro.AvroRelSchemaProvider;
import org.apache.samza.sql.avro.ConfigBasedAvroRelSchemaProviderFactory;
import org.apache.samza.sql.avro.schemas.Profile;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.serializers.SamzaSqlRelRecordSerdeFactory;
import org.apache.samza.system.SystemStream;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.sql.serializers.SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde;


public class TestSamzaSqlRelRecordSerde {

  private List<Object> values = Arrays.asList("value1", 1, null);
  private List<String> names = Arrays.asList("field1", "field2", "field3");

  @Test
  public void testWithDifferentFields() {
    SamzaSqlRelRecord record = new SamzaSqlRelMessage(names, values).getSamzaSqlRelRecord();
    SamzaSqlRelRecordSerde serde =
        (SamzaSqlRelRecordSerde) new SamzaSqlRelRecordSerdeFactory().getSerde(null, null);
    SamzaSqlRelRecord resultRecord = serde.fromBytes(serde.toBytes(record));
    Assert.assertEquals(names, resultRecord.getFieldNames());
    Assert.assertEquals(values, resultRecord.getFieldValues());
  }

  @Test
  public void testNestedRecordConversion() {
    Map<String, String> props = new HashMap<>();
    SystemStream ss1 = new SystemStream("test", "nestedRecord");
    props.put(
        String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA, ss1.getSystem(), ss1.getStream()),
        Profile.SCHEMA$.toString());
    ConfigBasedAvroRelSchemaProviderFactory factory = new ConfigBasedAvroRelSchemaProviderFactory();
    AvroRelSchemaProvider nestedRecordSchemaProvider =
        (AvroRelSchemaProvider) factory.create(ss1, new MapConfig(props));
    AvroRelConverter nestedRecordAvroRelConverter =
        new AvroRelConverter(ss1, nestedRecordSchemaProvider, new MapConfig());

    Pair<SamzaSqlRelMessage, GenericData.Record> messageRecordPair =
        TestSamzaSqlRelMessageSerde.createNestedSamzaSqlRelMessage(nestedRecordAvroRelConverter);
    SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde serde =
        (SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde) new SamzaSqlRelRecordSerdeFactory().getSerde(null, null);
    SamzaSqlRelRecord resultRecord = serde.fromBytes(serde.toBytes(messageRecordPair.getKey().getSamzaSqlRelRecord()));
    GenericData.Record recordPostConversion =
        (GenericData.Record) nestedRecordAvroRelConverter.convertToAvroObject(resultRecord, Profile.SCHEMA$);

    for (Schema.Field field : Profile.SCHEMA$.getFields()) {
      // equals() on GenericRecord does the nested record equality check as well.
      Assert.assertEquals(messageRecordPair.getValue().get(field.name()), recordPostConversion.get(field.name()));
    }
  }
}