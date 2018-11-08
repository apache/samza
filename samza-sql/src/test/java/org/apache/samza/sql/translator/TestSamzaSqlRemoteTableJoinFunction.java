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

package org.apache.samza.sql.translator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.avro.AvroRelConverter;
import org.apache.samza.sql.avro.AvroRelSchemaProvider;
import org.apache.samza.sql.avro.ConfigBasedAvroRelSchemaProviderFactory;
import org.apache.samza.sql.avro.schemas.SimpleRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelTableKeyConverter;
import org.apache.samza.sql.testutil.SampleRelTableKeyConverter;
import org.apache.samza.system.SystemStream;
import org.junit.Assert;
import org.junit.Test;

import static org.powermock.api.mockito.PowerMockito.*;


public class TestSamzaSqlRemoteTableJoinFunction {

  private List<String> streamFieldNames = Arrays.asList("field1", "field2", "field3", "field4");
  private List<Object> streamFieldValues = Arrays.asList("value1", 1, null, "value4");

  @Test
  public void testWithInnerJoinWithTableOnRight() {
    Map<String, String> props = new HashMap<>();
    SystemStream ss = new SystemStream("test", "nestedRecord");
    props.put(
        String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA, ss.getSystem(), ss.getStream()),
        SimpleRecord.SCHEMA$.toString());
    ConfigBasedAvroRelSchemaProviderFactory factory = new ConfigBasedAvroRelSchemaProviderFactory();
    AvroRelSchemaProvider schemaProvider =
        (AvroRelSchemaProvider) factory.create(ss, new MapConfig(props));
    AvroRelConverter relConverter =
        new AvroRelConverter(ss, schemaProvider, new MapConfig());
    SamzaRelTableKeyConverter relTableKeyConverter = new SampleRelTableKeyConverter();
    String remoteTableName = "testDb.testTable.$table";

    GenericData.Record tableRecord = new GenericData.Record(SimpleRecord.SCHEMA$);
    tableRecord.put("id", 1);
    tableRecord.put("name", "name1");

    SamzaSqlRelMessage streamMsg = new SamzaSqlRelMessage(streamFieldNames, streamFieldValues);
    SamzaSqlRelMessage tableMsg = relConverter.convertToRelMessage(new KV(tableRecord.get("id"), tableRecord));
    JoinRelType joinRelType = JoinRelType.INNER;
    List<Integer> streamKeyIds = Arrays.asList(1);
    List<Integer> tableKeyIds = Arrays.asList(0);
    KV<Object, GenericRecord> record = KV.of(tableRecord.get("id"), tableRecord);

    JoinInputNode mockTableInputNode = mock(JoinInputNode.class);
    when(mockTableInputNode.getKeyIds()).thenReturn(tableKeyIds);
    when(mockTableInputNode.isPosOnRight()).thenReturn(true);
    when(mockTableInputNode.getFieldNames()).thenReturn(tableMsg.getSamzaSqlRelRecord().getFieldNames());
    when(mockTableInputNode.getSourceName()).thenReturn(remoteTableName);

    JoinInputNode mockStreamInputNode = mock(JoinInputNode.class);
    when(mockStreamInputNode.getKeyIds()).thenReturn(streamKeyIds);
    when(mockStreamInputNode.isPosOnRight()).thenReturn(false);
    when(mockStreamInputNode.getFieldNames()).thenReturn(streamFieldNames);

    SamzaSqlRemoteTableJoinFunction joinFn =
        new SamzaSqlRemoteTableJoinFunction(relConverter, relTableKeyConverter, mockStreamInputNode, mockTableInputNode,
            joinRelType, 0);
    SamzaSqlRelMessage outMsg = joinFn.apply(streamMsg, record);

    Assert.assertEquals(outMsg.getSamzaSqlRelRecord().getFieldValues().size(),
        outMsg.getSamzaSqlRelRecord().getFieldNames().size());
    List<String> expectedFieldNames = new ArrayList<>(streamFieldNames);
    expectedFieldNames.addAll(tableMsg.getSamzaSqlRelRecord().getFieldNames());
    List<Object> expectedFieldValues = new ArrayList<>(streamFieldValues);
    expectedFieldValues.addAll(tableMsg.getSamzaSqlRelRecord().getFieldValues());

    Assert.assertEquals(expectedFieldNames, outMsg.getSamzaSqlRelRecord().getFieldNames());
    Assert.assertEquals(expectedFieldValues, outMsg.getSamzaSqlRelRecord().getFieldValues());
  }
}