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

package org.apache.samza.test.samzasql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.planner.SamzaSqlValidator;
import org.apache.samza.sql.planner.SamzaSqlValidatorException;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.system.TestAvroSystemFactory;
import org.apache.samza.sql.util.JsonUtil;
import org.apache.samza.sql.util.MyTestUdf;
import org.apache.samza.sql.util.SampleRelConverterFactory;
import org.apache.samza.sql.util.SamzaSqlTestConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestSamzaSqlEndToEnd extends SamzaSqlIntegrationTestHarness {
  private static final Logger LOG = LoggerFactory.getLogger(TestSamzaSqlEndToEnd.class);

  @Test
  public void testEndToEnd() throws SamzaSqlValidatorException {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    Assert.assertTrue(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()).equals(outMessages));
  }

  @Test
  public void testEndToEndWithSystemMessages() throws SamzaSqlValidatorException {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String avroSamzaToRelMsgConverterDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, "avro");
    staticConfigs.put(avroSamzaToRelMsgConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        SampleRelConverterFactory.class.getName());
    String sql = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
  }

  @Ignore
  @Test
  public void testEndToEndDisableSystemMessages() throws SamzaSqlValidatorException {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String avroSamzaToRelMsgConverterDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, "avro");
    staticConfigs.put(avroSamzaToRelMsgConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        SampleRelConverterFactory.class.getName());
    String sql = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_PROCESS_SYSTEM_EVENTS, "false");

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals((numMessages + 1) / 2, outMessages.size());
  }

  @Test
  public void testEndToEndWithNullRecords() throws SamzaSqlValidatorException {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(Collections.emptyMap(), numMessages, false, true);
    String sql = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> x.getMessage() == null || ((GenericRecord) x.getMessage()).get("id") == null ? null
            : Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .filter(Objects::nonNull)
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages - ((numMessages - 1) / TestAvroSystemFactory.NULL_RECORD_FREQUENCY + 1),
        outMessages.size());
    Assert.assertEquals(IntStream.range(0, numMessages)
        .boxed()
        .filter(x -> x % TestAvroSystemFactory.NULL_RECORD_FREQUENCY != 0)
        .collect(Collectors.toList()), outMessages);
  }

  @Test
  public void testEndToEndWithDifferentSystemSameStream() throws SamzaSqlValidatorException {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql = "Insert into testavro2.SIMPLE1 select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    Assert.assertTrue(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()).equals(outMessages));
  }

  @Test
  public void testEndToEndMultiSqlStmts() throws SamzaSqlValidatorException {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    String sql2 = "Insert into testavro.SIMPLE3 select * from testavro.SIMPLE2";
    List<String> sqlStmts = Arrays.asList(sql1, sql2);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages * 2, outMessages.size());
    Set<Integer> outMessagesSet = new HashSet<>(outMessages);
    Assert.assertEquals(numMessages, outMessagesSet.size());
    Assert.assertTrue(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()).equals(new ArrayList<>(outMessagesSet)));
  }

  @Test
  public void testEndToEndMultiSqlStmtsWithSameSystemStreamAsInputAndOutput() throws SamzaSqlValidatorException {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.SIMPLE1 select * from testavro.SIMPLE2";
    String sql2 = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1, sql2);

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages * 2, outMessages.size());
    Set<Integer> outMessagesSet = new HashSet<>(outMessages);
    Assert.assertEquals(numMessages, outMessagesSet.size());
    Assert.assertTrue(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()).equals(new ArrayList<>(outMessagesSet)));
  }

  @Test
  public void testEndToEndFanIn() throws SamzaSqlValidatorException {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE2";
    String sql2 = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1, sql2);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages * 2, outMessages.size());
    Set<Integer> outMessagesSet = new HashSet<>(outMessages);
    Assert.assertEquals(numMessages, outMessagesSet.size());
    Assert.assertTrue(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()).equals(new ArrayList<>(outMessagesSet)));
  }

  @Ignore
  @Test
  public void testEndToEndFanOut() throws SamzaSqlValidatorException {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.SIMPLE2 select * from testavro.SIMPLE1";
    String sql2 = "Insert into testavro.SIMPLE3 select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1, sql2);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages * 2, outMessages.size());
    Set<Integer> outMessagesSet = new HashSet<>(outMessages);
    Assert.assertEquals(numMessages, outMessagesSet.size());
    Assert.assertTrue(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()).equals(new ArrayList<>(outMessagesSet)));
  }

  @Test
  public void testEndToEndWithProjection() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic(id, bool_value, long_value) "
        + " select id, NOT(id = 5) as bool_value, TIMESTAMPDIFF(HOUR, CURRENT_TIMESTAMP(), LOCALTIMESTAMP()) + MONTH(CURRENT_DATE()) as long_value from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    Assert.assertEquals(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()), outMessages);
  }

  @Test
  public void testEndToEndWithBooleanCheck() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic"
        + " select * from testavro.COMPLEX1 where bool_value IS TRUE";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);
    Assert.assertEquals(numMessages / 2, outMessages.size());
  }

  @Test
  public void testEndToEndCompoundBooleanCheck() throws SamzaSqlValidatorException {

    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic"
        + " select * from testavro.COMPLEX1 where id >= 0 and bool_value IS TRUE";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);
    Assert.assertEquals(numMessages / 2, outMessages.size());
  }

  @Test
  public void testEndToEndCompoundBooleanCheckWorkaround() throws SamzaSqlValidatorException {

    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    // BUG Compound boolean checks dont work in calcite, So workaround by casting it to String
    String sql1 = "Insert into testavro.outputTopic"
        + " select * from testavro.COMPLEX1 where id >= 0 and CAST(bool_value AS VARCHAR) =  'TRUE'";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);

    Assert.assertEquals(10, outMessages.size());
  }

  @Test
  public void testEndToEndWithProjectionWithCase() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic(id, long_value) "
        + " select id, NOT(id = 5) as bool_value, CASE WHEN id IN (5, 6, 7) THEN CAST('foo' AS VARCHAR) WHEN id < 5 THEN CAST('bars' AS VARCHAR) ELSE NULL END as string_value from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    Assert.assertTrue(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()).equals(outMessages));
  }

  @Test
  public void testEndToEndWithLike() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic(id, bool_value, string_value) "
        + " select id, NOT(id = 5) as bool_value, name as string_value from testavro.SIMPLE1 where name like 'Name%'";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

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
        "Insert into testavro.outputTopic(string_value, id, bool_value, bytes_value, fixed_value, float_value0) "
            + " select Flatten(array_values) as string_value, id, NOT(id = 5) as bool_value, bytes_value, fixed_value, float_value0 "
            + " from testavro.COMPLEX1";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);

    int expectedMessages = 0;
    // Flatten de-normalizes the data. So there is separate record for each entry in the array.
    for (int index = 1; index < numMessages; index++) {
      expectedMessages = expectedMessages + Math.max(1, index);
    }
    Assert.assertEquals(expectedMessages, outMessages.size());
  }


  @Test
  public void testEndToEndComplexRecord() throws SamzaSqlValidatorException {
    int numMessages = 10;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);

    String sql1 =
        "Insert into testavro.outputTopic"
            + " select bool_value, map_values['key0'] as string_value, union_value, array_values, map_values, id, bytes_value,"
            + " fixed_value, float_value0 from testavro.COMPLEX1";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);

    Assert.assertEquals(numMessages, outMessages.size());
  }

  @Test
  public void testEndToEndWithFloatToStringConversion() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic"
        + " select 'urn:li:member:' || cast(cast(float_value0 as int) as varchar) as string_value, id, float_value0, "
        + " double_value, true as bool_value from testavro.COMPLEX1";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("string_value").toString().split(":")[3]))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    Assert.assertEquals(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()), outMessages);
  }

  @Ignore
  @Test
  public void testEndToEndNestedRecord() throws SamzaSqlValidatorException {
    int numMessages = 10;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);

    String sql1 =
        "Insert into testavro.outputTopic"
            + " select `phoneNumbers`[0].`kind`"
            + " from testavro.PROFILE as p";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);

    Assert.assertEquals(numMessages, outMessages.size());
  }

  @Test
  public void testEndToEndFlattenWithUdf() throws Exception {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 =
        "Insert into testavro.outputTopic(id, bool_value) select Flatten(MyTestArray(id)) as id, NOT(id = 5) as bool_value"
            + " from testavro.SIMPLE1";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

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
        "Insert into testavro.outputTopic(id, bool_value) select Flatten(a) as id, true as bool_value"
            + " from (select MyTestArray(id) a from testavro.SIMPLE1)";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);

    int expectedMessages = 0;
    // Flatten de-normalizes the data. So there is separate record for each entry in the array.
    for (int index = 1; index < numMessages; index++) {
      expectedMessages = expectedMessages + Math.max(1, index);
    }
    Assert.assertEquals(expectedMessages, outMessages.size());
  }

  @Test
  public void testUdfUnTypedArgumentToTypedUdf() throws SamzaSqlValidatorException {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic(id, bool_value, long_value) "
        + "select id, NOT(id = 5) as bool_value, MyTest(MyTestObj(id)) as long_value from testavro.SIMPLE1";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    LOG.info("output Messages " + TestAvroSystemFactory.messages);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("long_value").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(outMessages.size(), numMessages);
  }

  @Test(expected = SamzaSqlValidatorException.class)
  public void testMismatchedUdfArgumentTypeShouldFailWithException() {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic(long_value) "
            + "select MyTestObj(pageKey) as long_value from testavro.PAGEVIEW";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);
  }

  @Test
  public void testEndToEndUdf() throws Exception {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic(id, bool_value, long_value) "
        + "select id, NOT(id = 5) as bool_value, MYTest(id) as long_value from testavro.SIMPLE1;;";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

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
  public void testEndToEndUdfWithDisabledArgCheck() throws Exception {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.PROFILE1(id, address) "
        + "select id, BuildOutputRecord('key', GetNestedField(address, 'zip')) as address from testavro.PROFILE";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    LOG.info("output Messages " + TestAvroSystemFactory.messages);

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(outMessages.size(), numMessages);
  }

  @Test
  public void testEndToEndUdfPolymorphism() throws Exception {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql1 = "Insert into testavro.outputTopic(id, bool_value, long_value) "
        + "select MyTestPoly(id) as long_value, NOT(id = 5) as bool_value, MyTestPoly(name) as id from testavro.SIMPLE1";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

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
    String sql1 =
        "Insert into testavro.outputTopic(id, bool_value) "
            + "select id, NOT(id = 5) as bool_value "
            + "from testavro.SIMPLE1 "
            + "where RegexMatch('.*4', name)";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    LOG.info("output Messages " + TestAvroSystemFactory.messages);
    // There should be two messages that contain "4"
    Assert.assertEquals(TestAvroSystemFactory.messages.size(), 2);
  }

  @Test
  public void testEndToEndStreamTableInnerJoin() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.id = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
            ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileNameJoin(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test
  public void testEndToEndStreamTableInnerJoinWithPrimaryKey() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
            ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileNameJoin(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Ignore
  @Test
  public void testEndToEndStreamTableJoinWithSubQuery() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql =
        "Insert into testavro.enrichedPageViewTopic"
            + " select p.name as profileName, pv.pageKey as pageKey, p.address as profileAddress, coalesce(null, 'N/A') as companyName"
            + " from (SELECT * FROM (SELECT * from testavro.PAGEVIEW pv1 where pv1.profileId=0) as pv2) as pv"
            + " join testavro.PROFILE.`$table` as p"
            + " on p.id = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
            ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    Assert.assertEquals(1, outMessages.size());
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileNameJoin(1);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test
  public void testEndToEndStreamTableInnerJoinWithUdf() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on MyTest(p.id) = MyTest(pv.profileId)";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
            ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileNameJoin(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test
  public void testEndToEndStreamTableInnerJoinWithNestedRecord() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, p.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.id = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> {
            GenericRecord profileAddr = (GenericRecord) ((GenericRecord) x.getMessage()).get("profileAddress");
            GenericRecord streetNum = (GenericRecord) (profileAddr.get("streetnum"));
            return ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
                + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
                ((GenericRecord) x.getMessage()).get("profileName").toString()) + ","
                + profileAddr.get("zip") + "," + streetNum.get("number");
          })
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileNameAddressJoin(numMessages);
    Assert.assertEquals(outMessages, expectedOutMessages);
  }

  @Test
  public void testEndToEndStreamTableInnerJoinWithFilter() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, p.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.id = pv.profileId "
            + "where p.name = 'Mike'";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
            ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    Assert.assertEquals(4, outMessages.size());
    List<String> expectedOutMessages =
        TestAvroSystemFactory.getPageKeyProfileNameJoin(numMessages)
            .stream()
            .filter(msg -> msg.endsWith("Mike"))
            .collect(Collectors.toList());
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test
  public void testEndToEndStreamTableInnerJoinWithNullForeignKeys() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(Collections.emptyMap(), numMessages, true);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, p.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PAGEVIEW as pv "
            + "join testavro.PROFILE.`$table` as p "
            + " on pv.profileId = p.id";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
            ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    // Half the foreign keys are null.
    Assert.assertEquals(numMessages / 2, outMessages.size());
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileNameJoinWithNullForeignKeys(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test
  public void testEndToEndStreamTableLeftJoin() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(Collections.emptyMap(), numMessages, true);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, p.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PAGEVIEW as pv "
            + "left join testavro.PROFILE.`$table` as p "
            + " on pv.profileId = p.id";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
            ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    List<String> expectedOutMessages =
        TestAvroSystemFactory.getPageKeyProfileNameOuterJoinWithNullForeignKeys(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test
  public void testEndToEndStreamTableRightJoin() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(Collections.emptyMap(), numMessages, true);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, p.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "right join testavro.PAGEVIEW as pv "
            + " on p.id = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
            ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    List<String> expectedOutMessages =
        TestAvroSystemFactory.getPageKeyProfileNameOuterJoinWithNullForeignKeys(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Ignore
  @Test
  public void testEndToEndStreamTableTableJoin() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, c.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PAGEVIEW as pv "
            + "join testavro.PROFILE.`$table` as p "
            + " on MyTest(p.id) = MyTest(pv.profileId) "
            + " join testavro.COMPANY.`$table` as c "
            + " on MyTest(p.companyId) = MyTest(c.id)";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + ((GenericRecord) x.getMessage()).get("profileName").toString() + ","
            + ((GenericRecord) x.getMessage()).get("companyName").toString())
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileCompanyNameJoin(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test
  public void testEndToEndStreamTableTableJoinWithPrimaryKeys() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, c.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PAGEVIEW as pv "
            + "join testavro.PROFILE.`$table` as p "
            + " on MyTest(p.__key__) = MyTest(pv.profileId) "
            + " join testavro.COMPANY.`$table` as c "
            + " on MyTest(p.companyId) = MyTest(c.__key__)";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + ((GenericRecord) x.getMessage()).get("profileName").toString() + ","
            + ((GenericRecord) x.getMessage()).get("companyName").toString())
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileCompanyNameJoin(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test
  public void testEndToEndStreamTableTableJoinWithCompositeKey() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, c.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PAGEVIEW as pv "
            + "join testavro.PROFILE.`$table` as p "
            + " on p.id = pv.profileId "
            + " join testavro.COMPANY.`$table` as c "
            + " on p.companyId = c.id AND c.id = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + ((GenericRecord) x.getMessage()).get("profileName").toString() + ","
            + ((GenericRecord) x.getMessage()).get("companyName").toString())
        .collect(Collectors.toList());
    Assert.assertEquals(TestAvroSystemFactory.companies.length, outMessages.size());
    List<String> expectedOutMessages =
        TestAvroSystemFactory.getPageKeyProfileCompanyNameJoin(TestAvroSystemFactory.companies.length);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  // Disabling the test until SAMZA-1652 and SAMZA-1661 are fixed.
  @Ignore
  @Test
  public void testEndToEndGroupBy() throws Exception {
    int numMessages = 200;
    long windowDurationMs = 200;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(Collections.emptyMap(), numMessages, false, false,
            windowDurationMs);
    String sql =
        "Insert into testavro.pageViewCountTopic"
            + " select 'SampleJob' as jobName, pv.pageKey, count(*) as `count`"
            + " from testavro.PAGEVIEW as pv"
            + " where pv.pageKey = 'job' or pv.pageKey = 'inbox'"
            + " group by (pv.pageKey)";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    // Let's capture the list of windows/counts per key.
    HashMap<String, List<String>> pageKeyCountListMap = new HashMap<>();
    TestAvroSystemFactory.messages.stream()
        .map(x -> {
            String pageKey = ((GenericRecord) x.getMessage()).get("pageKey").toString();
            String count = ((GenericRecord) x.getMessage()).get("count").toString();
            pageKeyCountListMap.computeIfAbsent(pageKey, k -> new ArrayList<>()).add(count);
            return pageKeyCountListMap;
          });

    HashMap<String, Integer> pageKeyCountMap = new HashMap<>();
    pageKeyCountListMap.forEach((key, list) -> {
        // Check that the number of windows per key is non-zero but less than the number of input messages per key.
        Assert.assertTrue(list.size() > 1 && list.size() < numMessages / TestAvroSystemFactory.pageKeys.length);
        // Collapse the count of messages per key
        pageKeyCountMap.put(key, list.stream().mapToInt(Integer::parseInt).sum());
      });

    Set<String> pageKeys = new HashSet<>(Arrays.asList("job", "inbox"));
    HashMap<String, Integer> expectedPageKeyCountMap =
        TestAvroSystemFactory.getPageKeyGroupByResult(numMessages, pageKeys);

    Assert.assertEquals(expectedPageKeyCountMap, pageKeyCountMap);
  }
}
