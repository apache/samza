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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.planner.SamzaSqlValidator;
import org.apache.samza.sql.planner.SamzaSqlValidatorException;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.system.TestAvroSystemFactory;
import org.apache.samza.sql.util.JsonUtil;
import org.apache.samza.sql.util.SamzaSqlTestConfig;
import org.apache.samza.sql.util.RemoteStoreIOResolverTestFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class TestSamzaSqlRemoteTable extends SamzaSqlIntegrationTestHarness {
  @Test
  public void testSinkEndToEndWithKey() throws SamzaSqlValidatorException {
    int numMessages = 20;

    RemoteStoreIOResolverTestFactory.records.clear();

    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);

    String sql = "Insert into testRemoteStore.testTable.`$table` select __key__, id, name from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    Assert.assertEquals(numMessages, RemoteStoreIOResolverTestFactory.records.size());
  }

  @Test
  @Ignore("Disabled due to flakiness related to data generation; Refer Pull Request #905 for details")
  public void testSinkEndToEndWithKeyWithNullRecords() throws SamzaSqlValidatorException {
    int numMessages = 20;

    RemoteStoreIOResolverTestFactory.records.clear();

    Map<String, String> props = new HashMap<>();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(props, numMessages, false, true);

    String sql1 = "Insert into testRemoteStore.testTable.`$table` select __key__, id, name from testavro.SIMPLE1";

    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    Assert.assertEquals(numMessages, RemoteStoreIOResolverTestFactory.records.size());
  }

  @Test (expected = AssertionError.class)
  public void testSinkEndToEndWithoutKey() throws SamzaSqlValidatorException {
    int numMessages = 20;

    RemoteStoreIOResolverTestFactory.records.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);

    String sql = "Insert into testRemoteStore.testTable.`$table`(id,name) select id, name from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    Assert.assertEquals(numMessages, RemoteStoreIOResolverTestFactory.records.size());
  }

  @Test
  public void testJoinEndToEnd() throws SamzaSqlValidatorException {
    testJoinEndToEndHelper(false);
  }

  @Test
  public void testJoinEndToEndWithUdf() throws SamzaSqlValidatorException {
    testJoinEndToEndWithUdfHelper(false);
  }

  @Test
  public void testJoinEndToEndWithOptimizer() throws SamzaSqlValidatorException {
    testJoinEndToEndHelper(true);
  }

  @Test
  public void testJoinEndToEndWithUdfAndOptimizer() throws SamzaSqlValidatorException {
    testJoinEndToEndWithUdfHelper(true);
  }

  void testJoinEndToEndHelper(boolean enableOptimizer) throws SamzaSqlValidatorException {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    RemoteStoreIOResolverTestFactory.records.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    populateProfileTable(staticConfigs, numMessages);

    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId where p.name is not null";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_ENABLE_PLAN_OPTIMIZER, Boolean.toString(enableOptimizer));

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

  void testJoinEndToEndWithUdfHelper(boolean enableOptimizer) throws SamzaSqlValidatorException {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    RemoteStoreIOResolverTestFactory.records.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    populateProfileTable(staticConfigs, numMessages);

    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = BuildOutputRecord('id', pv.profileId)";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_ENABLE_PLAN_OPTIMIZER, Boolean.toString(enableOptimizer));

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
  public void testJoinEndToEndWithFilter() throws SamzaSqlValidatorException {
    testJoinEndToEndWithFilterHelper(false);
  }

  @Test
  public void testJoinEndToEndWithUdfAndFilter() throws SamzaSqlValidatorException {
    testJoinEndToEndWithUdfAndFilterHelper(false);
  }

  @Test
  public void testJoinEndToEndWithFilterAndOptimizer() throws SamzaSqlValidatorException {
    testJoinEndToEndWithFilterHelper(true);
  }

  @Test
  public void testJoinEndToEndWithUdfAndFilterAndOptimizer() throws SamzaSqlValidatorException {
    testJoinEndToEndWithUdfAndFilterHelper(true);
  }

  void testJoinEndToEndWithFilterHelper(boolean enableOptimizer) throws SamzaSqlValidatorException {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    RemoteStoreIOResolverTestFactory.records.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    populateProfileTable(staticConfigs, numMessages);

    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId"
            + " where p.name = 'Mike' and pv.profileId = 1";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_ENABLE_PLAN_OPTIMIZER, Boolean.toString(enableOptimizer));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
            ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    Assert.assertEquals(1, outMessages.size());
    Assert.assertEquals(outMessages.get(0), "home,Mike");
  }

  void testJoinEndToEndWithUdfAndFilterHelper(boolean enableOptimizer) throws SamzaSqlValidatorException {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    RemoteStoreIOResolverTestFactory.records.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    populateProfileTable(staticConfigs, numMessages);

    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = BuildOutputRecord('id', pv.profileId)"
            + " where p.name = 'Mike' and pv.profileId = 1";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_ENABLE_PLAN_OPTIMIZER, Boolean.toString(enableOptimizer));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
            ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    Assert.assertEquals(1, outMessages.size());
    Assert.assertEquals(outMessages.get(0), "home,Mike");
  }

  @Test
  public void testSourceEndToEndWithKeyWithNullForeignKeys() throws SamzaSqlValidatorException {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    RemoteStoreIOResolverTestFactory.records.clear();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(new HashMap<>(), numMessages, true);
    populateProfileTable(staticConfigs, numMessages);

    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
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
    Assert.assertEquals(numMessages / 2, outMessages.size());
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileNameJoinWithNullForeignKeys(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test
  public void testSourceEndToEndWithKeyWithNullForeignKeysRightOuterJoin() throws SamzaSqlValidatorException {
    int numMessages = 20;

    TestAvroSystemFactory.messages.clear();
    RemoteStoreIOResolverTestFactory.records.clear();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(new HashMap<>(), numMessages, true);
    populateProfileTable(staticConfigs, numMessages);

    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
            + "right join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId where p.name is null or  p.name <> '0'";

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
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileNameOuterJoinWithNullForeignKeys(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test(expected = SamzaException.class)
  public void testJoinConditionWithMoreThanOneConjunction() throws SamzaSqlValidatorException {
    int numMessages = 20;
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(new HashMap<>(), numMessages, true);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
            + "right join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId and p.__key__ = pv.pageKey where p.name is null or  p.name <> '0'";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);
    runApplication(config);
  }

  @Test(expected = SamzaException.class)
  public void testJoinConditionMissing__key__() throws SamzaSqlValidatorException {
    int numMessages = 20;
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(new HashMap<>(), numMessages, true);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
            + "right join testavro.PAGEVIEW as pv "
            + " on p.id = pv.profileId where p.name is null or  p.name <> '0'";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);
    runApplication(config);
  }


  @Test
  public void testSourceEndToEndWithFilterAndInnerJoin() throws SamzaSqlValidatorException {
    int numMessages = 20;
    TestAvroSystemFactory.messages.clear();
    RemoteStoreIOResolverTestFactory.records.clear();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(new HashMap<>(), numMessages, true);
    populateProfileTable(staticConfigs, numMessages);

    String sql = "Insert into testavro.enrichedPageViewTopic "
        + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
        + "       p.name as profileName, p.address as profileAddress "
        + "from testavro.PAGEVIEW as pv  "
        + "join testRemoteStore.Profile.`$table` as p "
        + " on p.__key__ = pv.profileId"
        + "  where p.name <> 'Mike' ";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + "," + (
            ((GenericRecord) x.getMessage()).get("profileName") == null ? "null"
                : ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileNameJoinWithNullForeignKeys(numMessages)
        .stream()
        .filter(x -> !x.contains("Mike"))
        .collect(Collectors.toList());
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test
  public void testSameJoinTargetSinkEndToEndRightOuterJoin() throws SamzaSqlValidatorException {
    int numMessages = 21;

    TestAvroSystemFactory.messages.clear();
    RemoteStoreIOResolverTestFactory.records.clear();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(new HashMap<>(), numMessages, true);
    populateProfileTable(staticConfigs, numMessages);

    // The below query reads messages from a stream and deletes the corresponding records from the table.
    // Since the stream has alternate messages with null foreign key, only half of the messages will have
    // successful joins and hence only half of the records in the table will be deleted. Although join is
    // redundant here, keeping it just for testing purpose.
    String sql =
        "Insert into testRemoteStore.Profile.`$table` "
            + "select p.__key__ as __key__, 'DELETE' as __op__ "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId ";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);

    runApplication(config);

    Assert.assertEquals((numMessages + 1) / 2, RemoteStoreIOResolverTestFactory.records.size());
  }

  @Test
  public void testDeleteOpValidation() throws SamzaSqlValidatorException {
    int numMessages = 1;
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(new HashMap<>(), numMessages, true);

    String sql =
        "Insert into testRemoteStore.Profile.`$table` "
            + "select p.__key__ as __key__, 'DELETE' as __op__ "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId ";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);
  }

  @Test (expected = SamzaSqlValidatorException.class)
  public void testUnsupportedOpValidation() throws SamzaSqlValidatorException {
    int numMessages = 1;
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(new HashMap<>(), numMessages, true);

    String sql =
        "Insert into testRemoteStore.Profile.`$table` "
            + "select p.__key__ as __key__, 'UPDATE' as __op__ "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);
  }

  @Test (expected = SamzaSqlValidatorException.class)
  public void testNonKeyWithDeleteOpValidation() throws SamzaSqlValidatorException {
    int numMessages = 1;
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(new HashMap<>(), numMessages, true);

    String sql =
        "Insert into testRemoteStore.Profile.`$table` "
            + "select p.__key__ as pageKey, 'UPDATE' as __op__ "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId ";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    Config config = new MapConfig(staticConfigs);
    new SamzaSqlValidator(config).validate(sqlStmts);
  }

  private void populateProfileTable(Map<String, String> staticConfigs, int numMessages) {
    RemoteStoreIOResolverTestFactory.records.clear();

    String sql = "Insert into testRemoteStore.Profile.`$table` select * from testavro.PROFILE";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    Assert.assertEquals(numMessages, RemoteStoreIOResolverTestFactory.records.size());
  }
}
