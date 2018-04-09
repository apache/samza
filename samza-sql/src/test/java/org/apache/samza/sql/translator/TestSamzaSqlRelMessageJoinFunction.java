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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.data.SamzaSqlCompositeKey;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.junit.Assert;
import org.junit.Test;


public class TestSamzaSqlRelMessageJoinFunction {

  private List<String> streamFieldNames = Arrays.asList("field1", "field2", "field3", "field4");
  private List<Object> streamFieldValues = Arrays.asList("value1", 1, null, "value4");
  private List<String> tableFieldNames = Arrays.asList("field11", "field12", "field13", "field14");
  private List<Object> tableFieldValues = Arrays.asList("value1", 1, null, "value5");

  @Test
  public void testWithInnerJoinWithTableOnRight() {
    SamzaSqlRelMessage streamMsg = new SamzaSqlRelMessage(streamFieldNames, streamFieldValues);
    SamzaSqlRelMessage tableMsg = new SamzaSqlRelMessage(tableFieldNames, tableFieldValues);
    JoinRelType joinRelType = JoinRelType.INNER;
    List<Integer> streamKeyIds = Arrays.asList(0, 1);
    List<Integer> tableKeyIds = Arrays.asList(0, 1);
    SamzaSqlCompositeKey compositeKey = SamzaSqlCompositeKey.createSamzaSqlCompositeKey(tableMsg, tableKeyIds);
    KV<SamzaSqlCompositeKey, SamzaSqlRelMessage> record = KV.of(compositeKey, tableMsg);

    SamzaSqlRelMessageJoinFunction joinFn =
        new SamzaSqlRelMessageJoinFunction(joinRelType, true, streamKeyIds, streamFieldNames, tableFieldNames);
    SamzaSqlRelMessage outMsg = joinFn.apply(streamMsg, record);

    Assert.assertEquals(outMsg.getFieldValues().size(), outMsg.getFieldNames().size());
    List<String> expectedFieldNames = new ArrayList<>(streamFieldNames);
    expectedFieldNames.addAll(tableFieldNames);
    List<Object> expectedFieldValues = new ArrayList<>(streamFieldValues);
    expectedFieldValues.addAll(tableFieldValues);
    Assert.assertEquals(outMsg.getFieldValues(), expectedFieldValues);
  }

  @Test
  public void testWithInnerJoinWithTableOnLeft() {
    SamzaSqlRelMessage streamMsg = new SamzaSqlRelMessage(streamFieldNames, streamFieldValues);
    SamzaSqlRelMessage tableMsg = new SamzaSqlRelMessage(tableFieldNames, tableFieldValues);
    JoinRelType joinRelType = JoinRelType.INNER;
    List<Integer> streamKeyIds = Arrays.asList(0, 2);
    List<Integer> tableKeyIds = Arrays.asList(0, 2);
    SamzaSqlCompositeKey compositeKey = SamzaSqlCompositeKey.createSamzaSqlCompositeKey(tableMsg, tableKeyIds);
    KV<SamzaSqlCompositeKey, SamzaSqlRelMessage> record = KV.of(compositeKey, tableMsg);

    SamzaSqlRelMessageJoinFunction joinFn =
        new SamzaSqlRelMessageJoinFunction(joinRelType, false, streamKeyIds, streamFieldNames, tableFieldNames);
    SamzaSqlRelMessage outMsg = joinFn.apply(streamMsg, record);

    Assert.assertEquals(outMsg.getFieldValues().size(), outMsg.getFieldNames().size());
    List<String> expectedFieldNames = new ArrayList<>(tableFieldNames);
    expectedFieldNames.addAll(streamFieldNames);
    List<Object> expectedFieldValues = new ArrayList<>(tableFieldValues);
    expectedFieldValues.addAll(streamFieldValues);
    Assert.assertEquals(outMsg.getFieldValues(), expectedFieldValues);
  }

  @Test
  public void testNullRecordWithInnerJoin() {
    SamzaSqlRelMessage streamMsg = new SamzaSqlRelMessage(streamFieldNames, streamFieldValues);
    JoinRelType joinRelType = JoinRelType.INNER;
    List<Integer> streamKeyIds = Arrays.asList(0, 1);

    SamzaSqlRelMessageJoinFunction joinFn =
        new SamzaSqlRelMessageJoinFunction(joinRelType, true, streamKeyIds, streamFieldNames, tableFieldNames);
    SamzaSqlRelMessage outMsg = joinFn.apply(streamMsg, null);
    Assert.assertNull(outMsg);
  }

  @Test
  public void testNullRecordWithLeftOuterJoin() {
    SamzaSqlRelMessage streamMsg = new SamzaSqlRelMessage(streamFieldNames, streamFieldValues);
    JoinRelType joinRelType = JoinRelType.LEFT;
    List<Integer> streamKeyIds = Arrays.asList(0, 1);

    SamzaSqlRelMessageJoinFunction joinFn =
        new SamzaSqlRelMessageJoinFunction(joinRelType, true, streamKeyIds, streamFieldNames,
            tableFieldNames);
    SamzaSqlRelMessage outMsg = joinFn.apply(streamMsg, null);

    Assert.assertEquals(outMsg.getFieldValues().size(), outMsg.getFieldNames().size());
    List<String> expectedFieldNames = new ArrayList<>(streamFieldNames);
    expectedFieldNames.addAll(tableFieldNames);
    List<Object> expectedFieldValues = new ArrayList<>(streamFieldValues);
    expectedFieldValues.addAll(tableFieldNames.stream().map( name -> null ).collect(Collectors.toList()));
    Assert.assertEquals(outMsg.getFieldValues(), expectedFieldValues);
  }
}