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

import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.testutil.JsonUtil;
import org.apache.samza.sql.testutil.SamzaSqlTestConfig;
import org.apache.samza.sql.testutil.TestSourceResolverFactory;
import org.junit.Assert;
import org.junit.Test;


public class TestSamzaSqlTable {
  @Test
  public void testEndToEnd() throws Exception {
    int numMessages = 20;

    TestSourceResolverFactory.TestTable.records.clear();

    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);

    String sql1 = "Insert into testDb.testTable.`$table` select id, name from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    runner.runAndWaitForFinish();

    Assert.assertEquals(numMessages, TestSourceResolverFactory.TestTable.records.size());
  }

  @Test
  public void testEndToEndWithKey() throws Exception {
    int numMessages = 20;

    TestSourceResolverFactory.TestTable.records.clear();
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);

    String sql1 = "Insert into testDb.testTable.`$table` select id __key__, name from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    runner.runAndWaitForFinish();

    Assert.assertEquals(numMessages, TestSourceResolverFactory.TestTable.records.size());
  }
}
