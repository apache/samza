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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.impl.ConfigBasedUdfResolver;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.testutil.JsonUtil;
import org.apache.samza.sql.dsls.samzasql.SamzaSqlQueryParser;
import org.apache.samza.sql.testutil.SamzaSqlTestConfig;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.sql.dsls.samzasql.SamzaSqlDslConverter.*;


public class TestSamzaSqlApplicationConfig {

  @Test
  public void testConfigInit() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, "Insert into testavro.COMPLEX1 select * from testavro.SIMPLE1");
    String configUdfResolverDomain = String.format(SamzaSqlApplicationConfig.CFG_FMT_UDF_RESOLVER_DOMAIN, "config");
    int numUdfs = config.get(configUdfResolverDomain + ConfigBasedUdfResolver.CFG_UDF_CLASSES).split(",").length;

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    Assert.assertEquals(numUdfs, samzaSqlApplicationConfig.getUdfMetadata().size());
    Assert.assertEquals(1, samzaSqlApplicationConfig.getInputSystemStreamConfigBySource().size());
    Assert.assertEquals(1, samzaSqlApplicationConfig.getOutputSystemStreamConfigsBySource().size());
  }

  @Test
  public void testWrongConfigs() {

    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);

    try {
      // Fail because no SQL config
      fetchSqlFromConfig(config);
      Assert.fail();
    } catch (SamzaException e) {
    }

    // Pass
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, "Insert into testavro.COMPLEX1 select * from testavro.SIMPLE1");

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    testWithoutConfigShouldFail(config, SamzaSqlApplicationConfig.CFG_IO_RESOLVER);
    testWithoutConfigShouldFail(config, SamzaSqlApplicationConfig.CFG_UDF_RESOLVER);

    String configIOResolverDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SOURCE_RESOLVER_DOMAIN, "config");
    String avroSamzaSqlConfigPrefix = configIOResolverDomain + String.format("%s.", "testavro");

    testWithoutConfigShouldFail(config, avroSamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_CONVERTER);

    // Configs for the unused system "log" is not mandatory.
    String logSamzaSqlConfigPrefix = configIOResolverDomain + String.format("%s.", "log");
    testWithoutConfigShouldPass(config, logSamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_CONVERTER);
  }

  @Test
  public void testGetInputAndOutputStreamConfigs() {
    List<String> sqlStmts = Arrays.asList("Insert into testavro.COMPLEX1 select * from testavro.SIMPLE1",
        "insert into testavro.Profile select * from testavro.SIMPLE1");
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    Set<String> inputKeys = samzaSqlApplicationConfig.getInputSystemStreamConfigBySource().keySet();
    Set<String> outputKeys = samzaSqlApplicationConfig.getOutputSystemStreamConfigsBySource().keySet();
    Assert.assertEquals(1, inputKeys.size());
    Assert.assertTrue(inputKeys.contains("testavro.SIMPLE1"));
    Assert.assertEquals(2, outputKeys.size());
    Assert.assertTrue(outputKeys.contains("testavro.COMPLEX1"));
    Assert.assertTrue(outputKeys.contains("testavro.Profile"));
  }

  private void testWithoutConfigShouldPass(Map<String, String> config, String configKey) {
    Map<String, String> badConfigs = new HashMap<>(config);
    badConfigs.remove(configKey);
    List<String> sqlStmts = fetchSqlFromConfig(badConfigs);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    new SamzaSqlApplicationConfig(new MapConfig(badConfigs),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));
  }

  private void testWithoutConfigShouldFail(Map<String, String> config, String configKey) {
    Map<String, String> badConfigs = new HashMap<>(config);
    badConfigs.remove(configKey);
    try {
      List<String> sqlStmts = fetchSqlFromConfig(badConfigs);
      List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
      new SamzaSqlApplicationConfig(new MapConfig(badConfigs),
          queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
              .collect(Collectors.toSet()),
          queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // swallow
    }
  }
}
