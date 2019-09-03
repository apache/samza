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

package org.apache.samza.sql.planner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.util.SamzaSqlTestConfig;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.sql.dsl.SamzaSqlDslConverter.*;


public class TestSamzaSqlValidator {

  private final Map<String, String> configs = new HashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(TestSamzaSqlValidator.class);

  @Before
  public void setUp() {
    configs.put("job.default.system", "kafka");
  }

  @Test
  public void testBasicValidation() throws SamzaSqlValidatorException {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(1);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
        "Insert into testavro.outputTopic(id) select id, name as string_value"
            + " from testavro.level1.level2.SIMPLE1 as s where s.id = 1");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    new SamzaSqlValidator(samzaConfig).validate(sqlStmts);
  }

  @Test (expected = SamzaSqlValidatorException.class)
  public void testNonExistingOutputField() throws SamzaSqlValidatorException {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(1);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
        "Insert into testavro.outputTopic(id) select id, name as strings_value"
            + " from testavro.level1.level2.SIMPLE1 as s where s.id = 1");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    new SamzaSqlValidator(samzaConfig).validate(sqlStmts);
  }

  @Test(expected = SamzaException.class)
  public void testNonExistingSelectField() throws SamzaSqlValidatorException {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(1);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
        "Insert into testavro.outputTopic(id) select non_existing_field, name as string_value"
            + " from testavro.level1.level2.SIMPLE1 as s where s.id = 1");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    new SamzaSqlValidator(samzaConfig).validate(sqlStmts);
  }

  @Test (expected = SamzaException.class)
  public void testNonExistingUdf() throws SamzaSqlValidatorException {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(1);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
        "Insert into testavro.outputTopic(id) select NonExistingUdf(name) as string_value"
            + " from testavro.level1.level2.SIMPLE1 as s where s.id = 1");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    new SamzaSqlValidator(samzaConfig).validate(sqlStmts);
  }

  @Test (expected = SamzaSqlValidatorException.class)
  public void testSelectAndOutputValidationFailure() throws SamzaSqlValidatorException {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(1);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
        "Insert into testavro.outputTopic(id) select name as long_value"
            + " from testavro.level1.level2.SIMPLE1 as s where s.id = 1");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    new SamzaSqlValidator(samzaConfig).validate(sqlStmts);
  }

  @Test (expected = SamzaException.class)
  public void testValidationStreamTableLeftJoinWithWhere() throws SamzaSqlValidatorException {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey) select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv left join testavro.PROFILE.`$table` as p where p.id = pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    new SamzaSqlValidator(samzaConfig).validate(sqlStmts);
  }

  @Test (expected = SamzaException.class)
  public void testUnsupportedOperator() throws SamzaSqlValidatorException {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.pageViewCountTopic(jobName, pageKey, `count`)"
            + " select 'SampleJob' as jobName, pv.pageKey, count(*) as `count`"
            + " from testavro.PAGEVIEW as pv"
            + " where pv.pageKey = 'job' or pv.pageKey = 'inbox'"
            + " group bys (pv.pageKey)";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    new SamzaSqlValidator(samzaConfig).validate(sqlStmts);
  }

  @Test
  public void testFormatErrorString() {
    String sql =
        "select 'SampleJob' as jobName, pv.pageKey, count(*) as `count`\n"
            + "from testavro.PAGEVIEW as pv\n"
            + "where pv.pageKey = 'job' or pv.pageKey = 'inbox'\n"
            + "group bys (pv.pageKey)";
    String errorStr =
        "org.apache.calcite.tools.ValidationException: org.apache.calcite.runtime.CalciteContextException: "
            + "From line 3, column 7 to line 3, column 16: Column 'pv.pageKey' not found in any table";
    String formattedErrStr = SamzaSqlValidator.formatErrorString(sql, new Exception(errorStr));
    LOG.info(formattedErrStr);
  }

  @Test
  public void testExceptionInFormatErrorString() {
    String sql =
        "select 'SampleJob' as jobName, pv.pageKey, count(*) as `count`\n"
            + "from testavro.PAGEVIEW as pv\n"
            + "where pv.pageKey = 'job' or pv.pageKey = 'inbox'\n"
            + "group bys (pv.pageKey)";
    String errorStr =
        "org.apache.calcite.tools.ValidationException: org.apache.calcite.runtime.CalciteContextException: "
            + "From line 3, column 7 to line 3, column 16: Column 'pv.pageKey' not found in any table";
    String formattedErrStr = SamzaSqlValidator.formatErrorString(sql, new Exception(errorStr));
    LOG.info(formattedErrStr);
  }
}
