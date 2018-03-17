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

package org.apache.samza.sql.e2e;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.system.TestAvroSystemFactory;
import org.apache.samza.sql.testutil.JsonUtil;
import org.apache.samza.sql.testutil.MyTestUdf;
import org.apache.samza.sql.testutil.SamzaSqlTestConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestSamzaSqlEndToEnd {

  private static final Logger LOG = LoggerFactory.getLogger(TestSamzaSqlEndToEnd.class);

  @Test
  public void testEndToEnd() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic select id, CURRENT_TIME as long_value from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    runner.runAndWaitForFinish();

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    Assert.assertTrue(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()).equals(outMessages));
  }

  @Test
  public void testEndToEndFlatten() throws Exception {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    LOG.info(" Class Path : " + RelOptUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
    String sql1 =
        "Insert into testavro.outputTopic select Flatten(array_values) as string_value, id from testavro.COMPLEX1";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    runner.runAndWaitForFinish();

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);

    int expectedMessages = 0;
    // Flatten de-normalizes the data. So there is separate record for each entry in the array.
    for (int index = 1; index < numMessages; index++) {
      expectedMessages = expectedMessages + Math.max(1, index);
    }
    Assert.assertEquals(expectedMessages, outMessages.size());
  }

  @Test
  public void testEndToEndSubQuery() throws Exception {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 =
        "Insert into testavro.outputTopic select Flatten(a) as id from (select MyTestArray(id) a from testavro.SIMPLE1)";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    runner.runAndWaitForFinish();

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);

    int expectedMessages = 0;
    // Flatten de-normalizes the data. So there is separate record for each entry in the array.
    for (int index = 1; index < numMessages; index++) {
      expectedMessages = expectedMessages + Math.max(1, index);
    }
    Assert.assertEquals(expectedMessages, outMessages.size());
  }

  @Test
  public void testEndToEndUdf() throws Exception {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic select id, MyTest(id) as long_value from testavro.SIMPLE1";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    runner.runAndWaitForFinish();

    LOG.info("output Messages " + TestAvroSystemFactory.messages);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("long_value").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(outMessages.size(), numMessages);
    MyTestUdf udf = new MyTestUdf();

    Assert.assertTrue(
        IntStream.range(0, numMessages).map(udf::execute).boxed().collect(Collectors.toList()).equals(outMessages));
  }

  @Test
  public void testRegexMatchUdfInWhereClause() throws Exception {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic select id from testavro.SIMPLE1 where RegexMatch('.*4', Name)";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    runner.runAndWaitForFinish();

    LOG.info("output Messages " + TestAvroSystemFactory.messages);
    // There should be two messages that contain "4"
    Assert.assertEquals(TestAvroSystemFactory.messages.size(), 2);
  }

}
