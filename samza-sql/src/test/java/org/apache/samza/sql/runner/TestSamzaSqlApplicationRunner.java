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

package org.apache.samza.sql.runner;

import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.runtime.RemoteApplicationRunner;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.testutil.SamzaSqlTestConfig;
import org.junit.Assert;

import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.junit.Test;


public class TestSamzaSqlApplicationRunner {

  @Test
  public void testComputeSamzaConfigs() {
    Map<String, String> configs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);
    String sql1 = "Insert into testavro.outputTopic select id, MyTest(id) as long_value from testavro.SIMPLE1";
    configs.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql1);
    configs.put(SamzaSqlApplicationRunner.RUNNER_CONFIG, SamzaSqlApplicationRunner.class.getName());
    MapConfig samzaConfig = new MapConfig(configs);
    Config newConfigs = SamzaSqlApplicationRunner.computeSamzaConfigs(true, samzaConfig);
    Assert.assertEquals(newConfigs.get(SamzaSqlApplicationRunner.RUNNER_CONFIG), LocalApplicationRunner.class.getName());
    // Check whether three new configs added.
    Assert.assertEquals(newConfigs.size(), configs.size() + 3);

    newConfigs = SamzaSqlApplicationRunner.computeSamzaConfigs(false, samzaConfig);
    Assert.assertEquals(newConfigs.get(SamzaSqlApplicationRunner.RUNNER_CONFIG), RemoteApplicationRunner.class.getName());

    // Check whether three new configs added.
    Assert.assertEquals(newConfigs.size(), configs.size() + 3);
  }
}
