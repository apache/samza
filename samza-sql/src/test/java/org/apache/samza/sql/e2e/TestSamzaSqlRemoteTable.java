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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.system.TestAvroSystemFactory;
import org.apache.samza.sql.testutil.JsonUtil;
import org.apache.samza.sql.testutil.SamzaSqlTestConfig;
import org.apache.samza.sql.testutil.RemoteStoreIOResolverTestFactory;
import org.junit.Assert;
import org.junit.Test;


public class TestSamzaSqlRemoteTable {
  @Test
  public void testSinkEndToEndWithKey() throws Exception {
    int numMessages = 20;

    RemoteStoreIOResolverTestFactory.records.clear();

    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);

    String sql = "Insert into testRemoteStore.testTable.`$table` select __key__, id, name from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner appRunnable = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    appRunnable.runAndWaitForFinish();

    Assert.assertEquals(numMessages, RemoteStoreIOResolverTestFactory.records.size());
  }

  @Test (expected = AssertionError.class)
  public void testSinkEndToEndWithoutKey() throws Exception {
    int numMessages = 20;

    RemoteStoreIOResolverTestFactory.records.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);

    String sql = "Insert into testRemoteStore.testTable.`$table`(id,name) select id, name from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner appRunnable = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    appRunnable.runAndWaitForFinish();

    Assert.assertEquals(numMessages, RemoteStoreIOResolverTestFactory.records.size());
  }

  @Test
  public void testSourceEndToEndWithKey() throws Exception {
    int numMessages = 20;

    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);
    populateProfileTable(staticConfigs);

    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId";

    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner appRunnable = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    appRunnable.runAndWaitForFinish();

    List<String> outMessages = TestAvroSystemFactory.messages.stream()
        .map(x -> ((GenericRecord) x.getMessage()).get("pageKey").toString() + ","
            + (((GenericRecord) x.getMessage()).get("profileName") == null ? "null" :
            ((GenericRecord) x.getMessage()).get("profileName").toString()))
        .collect(Collectors.toList());
    Assert.assertEquals(numMessages, outMessages.size());
    List<String> expectedOutMessages = TestAvroSystemFactory.getPageKeyProfileNameJoin(numMessages);
    Assert.assertEquals(expectedOutMessages, outMessages);
  }

  private void populateProfileTable(Map<String, String> staticConfigs) {
    int numMessages = 20;

    RemoteStoreIOResolverTestFactory.records.clear();

    String sql = "Insert into testRemoteStore.Profile.`$table` select * from testavro.PROFILE";
    List<String> sqlStmts = Arrays.asList(sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner appRunnable = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    appRunnable.runAndWaitForFinish();

    Assert.assertEquals(numMessages, RemoteStoreIOResolverTestFactory.records.size());
  }
}
