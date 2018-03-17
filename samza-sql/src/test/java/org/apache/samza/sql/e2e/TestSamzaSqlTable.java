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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestSamzaSqlTable {
  private static final Logger LOG = LoggerFactory.getLogger(TestSamzaSqlEndToEnd.class);

  @Test
  public void testEndToEnd() throws Exception {
    int numMessages = 20;

    TestSourceResolverFactory.TestTable.records.clear();

    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(numMessages);

    String sql1 = "Insert into testDb.testTable select id, name from testavro.SIMPLE1";
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

    String sql1 = "Insert into testDb.testTable select id __key__, name from testavro.SIMPLE1";
    List<String> sqlStmts = Arrays.asList(sql1);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    runner.runAndWaitForFinish();

    Assert.assertEquals(numMessages, TestSourceResolverFactory.TestTable.records.size());
  }
}
