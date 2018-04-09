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

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.impl.ConfigBasedUdfResolver;
import org.apache.samza.sql.interfaces.SqlSystemSourceConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.testutil.SamzaSqlTestConfig;
import org.junit.Assert;
import org.junit.Test;


public class TestSamzaSqlApplicationConfig {

  @Test
  public void testConfigInit() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, "Insert into testavro.COMPLEX1 select * from testavro.SIMPLE1");
    String configUdfResolverDomain = String.format(SamzaSqlApplicationConfig.CFG_FMT_UDF_RESOLVER_DOMAIN, "config");
    int numUdfs = config.get(configUdfResolverDomain + ConfigBasedUdfResolver.CFG_UDF_CLASSES).split(",").length;
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config));
    Assert.assertEquals(1, samzaSqlApplicationConfig.getQueryInfo().size());
    Assert.assertEquals(numUdfs, samzaSqlApplicationConfig.getUdfMetadata().size());
    Assert.assertEquals(1, samzaSqlApplicationConfig.getInputSystemStreamConfigBySource().size());
    Assert.assertEquals(1, samzaSqlApplicationConfig.getOutputSystemStreamConfigsBySource().size());
  }

  @Test
  public void testWrongConfigs() {

    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);


    try {
      // Fail because no SQL config
      new SamzaSqlApplicationConfig(new MapConfig(config));
      Assert.fail();
    } catch (SamzaException e) {
    }

    // Pass
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, "Insert into testavro.COMPLEX1 select * from testavro.SIMPLE1");
    new SamzaSqlApplicationConfig(new MapConfig(config));
    testWithoutConfigShouldFail(config, SamzaSqlApplicationConfig.CFG_SOURCE_RESOLVER);
    testWithoutConfigShouldFail(config, SamzaSqlApplicationConfig.CFG_UDF_RESOLVER);

    String configSourceResolverDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SOURCE_RESOLVER_DOMAIN, "config");
    String avroSamzaSqlConfigPrefix = configSourceResolverDomain + String.format("%s.", "testavro");

    testWithoutConfigShouldFail(config, avroSamzaSqlConfigPrefix + SqlSystemSourceConfig.CFG_SAMZA_REL_CONVERTER);

    // Configs for the unused system "log" is not mandatory.
    String logSamzaSqlConfigPrefix = configSourceResolverDomain + String.format("%s.", "log");
    testWithoutConfigShouldPass(config, logSamzaSqlConfigPrefix + SqlSystemSourceConfig.CFG_SAMZA_REL_CONVERTER);
  }

  private void testWithoutConfigShouldPass(Map<String, String> config, String configKey) {
    Map<String, String> badConfigs = new HashMap<>(config);
    badConfigs.remove(configKey);
    new SamzaSqlApplicationConfig(new MapConfig(badConfigs));
  }

  private void testWithoutConfigShouldFail(Map<String, String> config, String configKey) {
    Map<String, String> badConfigs = new HashMap<>(config);
    badConfigs.remove(configKey);
    try {
      new SamzaSqlApplicationConfig(new MapConfig(badConfigs));
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // swallow
    }
  }
}
