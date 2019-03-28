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
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.system.TestAvroSystemFactory;
import org.apache.samza.sql.util.JsonUtil;
import org.apache.samza.sql.util.SamzaSqlTestConfig;
import org.apache.samza.sql.util.RemoteStoreIOResolverTestFactory;
import org.junit.Assert;
import org.junit.Test;


public class TestSamzaSqlRemoteTable extends SamzaSqlIntegrationTestHarness {
  @Test
  public void testSinkEndToEndWithKey() {
    int numMessages = 20;

    RemoteStoreIOResolverTestFactory.records.clear();

    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);

    String sql = "Insert into testRemoteStore.testTable.`$table` select __key__, id, name from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    Assert.assertEquals(numMessages, RemoteStoreIOResolverTestFactory.records.size());
  }

  @Test
  public void testSinkEndToEndWithKeyWithNullRecords() {
    int numMessages = 20;

    RemoteStoreIOResolverTestFactory.records.clear();

    Map<String, String> props = new HashMap<>();
    Map<String, String> staticConfigs =
        SamzaSqlTestConfig.fetchStaticConfigsWithFactories(props, numMessages, false, true);

    String sql1 = "Insert into testRemoteStore.testTable.`$table` select __key__, id, name from testavro.SIMPLE1";
    String sql2 = "Insert into testRemoteStore.testTable.`$table` select __key__, 'DELETE' as __op__ from testavro.SIMPLE1 WHERE name IS NULL";

    List<String> sqlStmts = Arrays.asList(sql1, sql2);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    Assert.assertEquals(numMessages - ((numMessages - 1) / TestAvroSystemFactory.NULL_RECORD_FREQUENCY + 1),
        RemoteStoreIOResolverTestFactory.records.size());
  }

  @Test (expected = AssertionError.class)
  public void testSinkEndToEndWithoutKey() {
    int numMessages = 20;

    RemoteStoreIOResolverTestFactory.records.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);

    String sql = "Insert into testRemoteStore.testTable.`$table`(id,name) select id, name from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    runApplication(new MapConfig(staticConfigs));

    Assert.assertEquals(numMessages, RemoteStoreIOResolverTestFactory.records.size());
  }

  @Test
  public void testSourceEndToEndWithKey() {
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
  public void testSourceEndToEndWithKeyAndUdf() {
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
  public void testSourceEndToEndWithKeyWithNullForeignKeys() {
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
    runApplication(new MapConfig(staticConfigs));

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
  public void testSourceEndToEndWithKeyWithNullForeignKeysRightOuterJoin() {
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
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileNameOuterJoinWithNullForeignKeys(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  @Test
  public void testSameJoinTargetSinkEndToEndRightOuterJoin() {
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
    runApplication(new MapConfig(staticConfigs));

    Assert.assertEquals((numMessages + 1) / 2, RemoteStoreIOResolverTestFactory.records.size());
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
