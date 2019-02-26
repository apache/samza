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
import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.JsonSerdeV2Factory;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.system.TestAvroSystemFactory;
import org.apache.samza.sql.util.JsonUtil;
import org.apache.samza.sql.util.MyTestUdf;
import org.apache.samza.sql.util.SampleRelConverterFactory;
import org.apache.samza.sql.util.SamzaSqlTestConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestSamzaSqlEndToEnd extends SamzaSqlIntegrationTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestSamzaSqlEndToEnd.class);
  private final Map<String, String> configs = new HashMap<>();

  @Before
  public void setUp() {
    super.setUp();
    configs.put("systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory");
    configs.put("systems.kafka.producer.bootstrap.servers", bootstrapUrl());
    configs.put("systems.kafka.consumer.zookeeper.connect", zkConnect());
    configs.put("systems.kafka.samza.key.serde", "object");
    configs.put("systems.kafka.samza.msg.serde", "samzaSqlRelMsg");
    configs.put("systems.kafka.default.stream.replication.factor", "1");
    configs.put("job.default.system", "kafka");

    configs.put("serializers.registry.object.class", JsonSerdeV2Factory.class.getName());
    configs.put("serializers.registry.samzaSqlRelMsg.class", JsonSerdeV2Factory.class.getName());
  }

  @Test
  public void testEndToEnd() {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String sql = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    Assert.assertTrue(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()).equals(outMessages));
  }

  @Test
  public void testEndToEndWithSystemMessages() {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String avroSamzaToRelMsgConverterDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, "avro");
    staticConfigs.put(avroSamzaToRelMsgConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        SampleRelConverterFactory.class.getName());
    String sql = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals((numMessages + 1) / 2, outMessages.size());
  }

  @Test
  public void testEndToEndWithNullRecords() {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages, false, true);
    String sql = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
  public void testEndToEndWithDifferentSystemSameStream() {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String sql = "Insert into testavro2.SIMPLE1 select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    List<Integer> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> Integer.valueOf(((GenericRecord) x.getMessage()).get("id").toString()))
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    Assert.assertTrue(IntStream.range(0, numMessages).boxed().collect(Collectors.toList()).equals(outMessages));
  }

  @Test
  public void testEndToEndMultiSqlStmts() {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String sql1 = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    String sql2 = "Insert into testavro.SIMPLE3 select * from testavro.SIMPLE2";
    List<String> sqlStmts = Arrays.asList(sql1, sql2);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));
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
  public void testEndToEndMultiSqlStmtsWithSameSystemStreamAsInputAndOutput() {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String sql1 = "Insert into testavro.SIMPLE1 select * from testavro.SIMPLE2";
    String sql2 = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1, sql2);

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));
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
  public void testEndToEndFanIn() {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String sql1 = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE2";
    String sql2 = "Insert into testavro.simpleOutputTopic select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1, sql2);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));
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
  public void testEndToEndFanOut() {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String sql1 = "Insert into testavro.SIMPLE2 select * from testavro.SIMPLE1";
    String sql2 = "Insert into testavro.SIMPLE3 select * from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1, sql2);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));
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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String sql1 = "Insert into testavro.outputTopic(id, long_value) "
        + " select id, TIMESTAMPDIFF(HOUR, CURRENT_TIMESTAMP, LOCALTIMESTAMP) + MONTH(CURRENT_DATE) as long_value from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);

    LOG.info(" Class Path : " + RelOptUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
    String sql1 =
        "Insert into testavro.outputTopic(string_value, id, bytes_value, fixed_value, float_value) "
            + " select Flatten(array_values) as string_value, id, bytes_value, fixed_value, float_value "
            + " from testavro.COMPLEX1";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);

    int expectedMessages = 0;
    // Flatten de-normalizes the data. So there is separate record for each entry in the array.
    for (int index = 1; index < numMessages; index++) {
      expectedMessages = expectedMessages + Math.max(1, index);
    }
    Assert.assertEquals(expectedMessages, outMessages.size());
  }


  @Test
  public void testEndToEndComplexRecord() {
    int numMessages = 10;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);

    String sql1 =
        "Insert into testavro.outputTopic"
            + " select map_values['key0'] as string_value, union_value, array_values[0] as string_value, map_values, id, bytes_value, fixed_value, float_value "
            + " from testavro.COMPLEX1";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);

    Assert.assertEquals(numMessages, outMessages.size());
  }

  @Ignore
  @Test
  public void testEndToEndNestedRecord() {
    int numMessages = 10;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);

    String sql1 =
        "Insert into testavro.outputTopic"
            + " select `phoneNumbers`[0].`kind`"
            + " from testavro.PROFILE as p";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    List<OutgoingMessageEnvelope> outMessages = new ArrayList<>(TestAvroSystemFactory.messages);

    Assert.assertEquals(numMessages, outMessages.size());
  }

  @Test
  public void testEndToEndSubQuery() throws Exception {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String sql1 =
        "Insert into testavro.outputTopic(id) select Flatten(a) as id from (select MyTestArray(id) a from testavro.SIMPLE1)";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String sql1 = "Insert into testavro.outputTopic(id, long_value) "
        + "select id, MyTest(id) as long_value from testavro.SIMPLE1";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
  public void testEndToEndUdfPolymorphism() throws Exception {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String sql1 = "Insert into testavro.outputTopic(id, long_value) "
        + "select MyTestPoly(id) as long_value, MyTestPoly(name) as id from testavro.SIMPLE1";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    String sql1 =
        "Insert into testavro.outputTopic(id) "
            + "select id "
            + "from testavro.SIMPLE1 "
            + "where RegexMatch('.*4', name)";
    List<String> sqlStmts = Collections.singletonList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    LOG.info("output Messages " + TestAvroSystemFactory.messages);
    // There should be two messages that contain "4"
    Assert.assertEquals(TestAvroSystemFactory.messages.size(), 2);
  }

  @Test
  public void testEndToEndStreamTableInnerJoin() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    staticConfigs.putAll(configs);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.id = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    staticConfigs.putAll(configs);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
  public void testEndToEndStreamTableInnerJoinWithUdf() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    staticConfigs.putAll(configs);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on MyTest(p.id) = MyTest(pv.profileId)";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    staticConfigs.putAll(configs);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, p.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.id = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
    staticConfigs.putAll(configs);
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
    runApplication(new MapConfig(staticConfigs));

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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages, true);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, p.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PAGEVIEW as pv "
            + "join testavro.PROFILE.`$table` as p "
            + " on pv.profileId = p.id";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages, true);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, p.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PAGEVIEW as pv "
            + "left join testavro.PROFILE.`$table` as p "
            + " on pv.profileId = p.id";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages, true);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, p.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "right join testavro.PAGEVIEW as pv "
            + " on p.id = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
  public void testEndToEndStreamTableTableJoin() throws Exception {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
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
    runApplication(new MapConfig(staticConfigs));

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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
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
    runApplication(new MapConfig(staticConfigs));

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
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages);
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
    runApplication(new MapConfig(staticConfigs));

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
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, numMessages, false, false, windowDurationMs);
    staticConfigs.putAll(configs);
    String sql =
        "Insert into testavro.pageViewCountTopic"
            + " select 'SampleJob' as jobName, pv.pageKey, count(*) as `count`"
            + " from testavro.PAGEVIEW as pv"
            + " where pv.pageKey = 'job' or pv.pageKey = 'inbox'"
            + " group by (pv.pageKey)";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

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
