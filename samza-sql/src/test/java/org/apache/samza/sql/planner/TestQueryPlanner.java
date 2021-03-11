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

import java.util.Collection;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.dsl.SamzaSqlDslConverterFactory;
import org.apache.samza.sql.interfaces.DslConverter;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.util.SamzaSqlTestConfig;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestQueryPlanner {

  @Test
  public void testTranslate() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);
    String sql =
        "Insert into testavro.outputTopic(id) select MyTest(id) from testavro.level1.level2.SIMPLE1 as s where s.id = 10";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    DslConverter dslConverter = new SamzaSqlDslConverterFactory().create(samzaConfig);
    Collection<RelRoot> relRoots = dslConverter.convertDsl(sql);
    assertEquals(1, relRoots.size());
  }

  @Test
  public void testRemoteJoinWithFilter() throws SamzaSqlValidatorException {
    testRemoteJoinWithFilterHelper(false);
  }

  @Test
  public void testRemoteJoinWithUdfAndFilter() throws SamzaSqlValidatorException {
    testRemoteJoinWithUdfAndFilterHelper(false);
  }

  @Test
  public void testRemoteJoinWithFilterAndOptimizer() throws SamzaSqlValidatorException {
    testRemoteJoinWithFilterHelper(true);
  }

  @Test
  public void testRemoteJoinWithUdfAndFilterAndOptimizer() throws SamzaSqlValidatorException {
    testRemoteJoinWithUdfAndFilterHelper(true);
  }

  void testRemoteJoinWithFilterHelper(boolean enableOptimizer) throws SamzaSqlValidatorException {
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(1);

    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testavro.PAGEVIEW as pv "
            + "join testRemoteStore.Profile.`$table` as p "
            + " on p.__key__ = pv.profileId"
            + " where p.name = pv.pageKey AND p.name = 'Mike' AND pv.profileId = 1";

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_ENABLE_PLAN_OPTIMIZER, Boolean.toString(enableOptimizer));

    Config samzaConfig = new MapConfig(staticConfigs);
    DslConverter dslConverter = new SamzaSqlDslConverterFactory().create(samzaConfig);
    Collection<RelRoot> relRoots = dslConverter.convertDsl(sql);

    /*
      Query plan without optimization:
      LogicalProject(__key__=[$1], pageKey=[$1], companyName=['N/A'], profileName=[$5], profileAddress=[$7])
        LogicalFilter(condition=[AND(=($5, $1), =($5, 'Mike'), =($2, 1))])
          LogicalJoin(condition=[=($3, $2)], joinType=[inner])
            LogicalTableScan(table=[[testavro, PAGEVIEW]])
            LogicalTableScan(table=[[testRemoteStore, Profile, $table]])

      Query plan with optimization:
      LogicalProject(__key__=[$1], pageKey=[$1], companyName=['N/A'], profileName=[$5], profileAddress=[$7])
        LogicalFilter(condition=[AND(=($5, $1), =($5, 'Mike'))])
          LogicalJoin(condition=[=($3, $2)], joinType=[inner])
            LogicalFilter(condition=[=($2, 1)])
              LogicalTableScan(table=[[testavro, PAGEVIEW]])
            LogicalTableScan(table=[[testRemoteStore, Profile, $table]])
     */

    assertEquals(1, relRoots.size());
    RelRoot relRoot = relRoots.iterator().next();
    RelNode relNode = relRoot.rel;
    assertTrue(relNode instanceof LogicalProject);
    relNode = relNode.getInput(0);
    assertTrue(relNode instanceof LogicalFilter);
    if (enableOptimizer) {
      assertEquals("AND(=($1, $5), =($5, 'Mike'))", ((LogicalFilter) relNode).getCondition().toString());
    } else {
      assertEquals("AND(=(1, $2), =($1, $5), =($5, 'Mike'))", ((LogicalFilter) relNode).getCondition().toString());
    }
    relNode = relNode.getInput(0);
    assertTrue(relNode instanceof LogicalJoin);
    assertEquals(2, relNode.getInputs().size());
    LogicalJoin join = (LogicalJoin) relNode;
    RelNode left = join.getLeft();
    RelNode right = join.getRight();
    assertTrue(right instanceof LogicalTableScan);
    if (enableOptimizer) {
      assertTrue(left instanceof LogicalFilter);
      assertEquals("=(1, $2)", ((LogicalFilter) left).getCondition().toString());
      assertTrue(left.getInput(0) instanceof LogicalTableScan);
    } else {
      assertTrue(left instanceof LogicalTableScan);
    }
  }

  void testRemoteJoinWithUdfAndFilterHelper(boolean enableOptimizer) throws SamzaSqlValidatorException {
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(1);

    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = BuildOutputRecord('id', pv.profileId)"
            + " where p.name = 'Mike' and pv.profileId = 1 and p.name = pv.pageKey";

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_ENABLE_PLAN_OPTIMIZER, Boolean.toString(enableOptimizer));

    Config samzaConfig = new MapConfig(staticConfigs);
    DslConverter dslConverter = new SamzaSqlDslConverterFactory().create(samzaConfig);
    Collection<RelRoot> relRoots = dslConverter.convertDsl(sql);

    /*
      Query plan without optimization:
      LogicalProject(__key__=[$9], pageKey=[$9], companyName=['N/A'], profileName=[$2], profileAddress=[$4])
        LogicalFilter(condition=[AND(=($2, 'Mike'), =($10, 1), =($2, $9))])  ==> Only the second condition could be pushed down.
          LogicalProject(__key__=[$0], id=[$1], name=[$2], companyId=[$3], address=[$4], selfEmployed=[$5],
                                  phoneNumbers=[$6], mapValues=[$7], __key__0=[$8], pageKey=[$9], profileId=[$10])
                                  ==> ProjectMergeRule removes this redundant node.
            LogicalJoin(condition=[=($0, $11)], joinType=[inner])
              LogicalTableScan(table=[[testRemoteStore, Profile, $table]])
              LogicalProject(__key__=[$0], pageKey=[$1], profileId=[$2], $f3=[BuildOutputRecord('id', $2)])  ==> Filter is pushed above project.
                LogicalTableScan(table=[[testavro, PAGEVIEW]])

      Query plan with optimization:
      LogicalProject(__key__=[$9], pageKey=[$9], companyName=['N/A'], profileName=[$2], profileAddress=[$4])
          LogicalFilter(condition=[AND(=($2, 'Mike'), =($2, $9))])
            LogicalJoin(condition=[=($0, $11)], joinType=[inner])
              LogicalTableScan(table=[[testRemoteStore, Profile, $table]])
              LogicalFilter(condition=[=($2, 1)])
                LogicalProject(__key__=[$0], pageKey=[$1], profileId=[$2], $f3=[BuildOutputRecord('id', $2)])
                  LogicalTableScan(table=[[testavro, PAGEVIEW]])
     */

    assertEquals(1, relRoots.size());
    RelRoot relRoot = relRoots.iterator().next();
    RelNode relNode = relRoot.rel;
    assertTrue(relNode instanceof LogicalProject);
    relNode = relNode.getInput(0);
    assertTrue(relNode instanceof LogicalFilter);
    if (enableOptimizer) {
      assertEquals("AND(=($2, $9), =($2, 'Mike'))", ((LogicalFilter) relNode).getCondition().toString());
    } else {
      assertEquals("AND(=($2, $9), =(1, $10), =($2, 'Mike'))", ((LogicalFilter) relNode).getCondition().toString());
    }
    relNode = relNode.getInput(0);
    if (enableOptimizer) {
      assertTrue(relNode instanceof LogicalJoin);
      assertEquals(2, relNode.getInputs().size());
    } else {
      assertTrue(relNode instanceof LogicalProject);
      relNode = relNode.getInput(0);
    }
    LogicalJoin join = (LogicalJoin) relNode;
    RelNode left = join.getLeft();
    RelNode right = join.getRight();
    assertTrue(left instanceof LogicalTableScan);
    if (enableOptimizer) {
      assertTrue(right instanceof LogicalFilter);
      assertEquals("=(1, $2)", ((LogicalFilter) right).getCondition().toString());
      relNode = right.getInput(0);
    } else {
      relNode = right;
    }
    assertTrue(relNode instanceof LogicalProject);
    relNode = relNode.getInput(0);
    assertTrue(relNode instanceof LogicalTableScan);
  }

  @Test
  public void testLocalStreamTableInnerJoinFilterOptimization() throws Exception {
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, p.name as companyName, p.name as profileName,"
            + "       p.address as profileAddress "
            + "from testavro.PROFILE.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.id = pv.profileId "
            + "where p.name = 'Mike'";

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_ENABLE_PLAN_OPTIMIZER, Boolean.toString(true));

    Config samzaConfig = new MapConfig(staticConfigs);
    DslConverter dslConverter = new SamzaSqlDslConverterFactory().create(samzaConfig);
    Collection<RelRoot> relRootsWithOptimization = dslConverter.convertDsl(sql);

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_ENABLE_PLAN_OPTIMIZER, Boolean.toString(false));

    samzaConfig = new MapConfig(staticConfigs);
    dslConverter = new SamzaSqlDslConverterFactory().create(samzaConfig);
    Collection<RelRoot> relRootsWithoutOptimization = dslConverter.convertDsl(sql);

    // We do not yet have any join filter optimizations for local joins. Hence the plans with and without optimization
    // should be the same.
    assertEquals(RelOptUtil.toString(relRootsWithOptimization.iterator().next().rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES),
        RelOptUtil.toString(relRootsWithoutOptimization.iterator().next().rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
  }

  @Test
  public void testRemoteJoinFilterPushDownWithUdfInFilterAndOptimizer() throws SamzaSqlValidatorException {
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(1);

    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId"
            + " where p.name = pv.pageKey AND p.name = 'Mike' AND pv.profileId = MyTest(pv.profileId)";

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_ENABLE_PLAN_OPTIMIZER, Boolean.toString(true));

    Config samzaConfig = new MapConfig(staticConfigs);
    DslConverter dslConverter = new SamzaSqlDslConverterFactory().create(samzaConfig);
    Collection<RelRoot> relRoots = dslConverter.convertDsl(sql);

    /*
      Query plan without optimization:
      LogicalProject(__key__=[$9], pageKey=[$9], companyName=['N/A'], profileName=[$2], profileAddress=[$4])
        LogicalFilter(condition=[AND(=($2, $9), =($2, 'Mike'), =($10, CAST(MyTest($10)):INTEGER))])
          LogicalJoin(condition=[=($0, $10)], joinType=[inner])
            LogicalTableScan(table=[[testRemoteStore, Profile, $table]])
            LogicalTableScan(table=[[testavro, PAGEVIEW]])

      Query plan with optimization:
      LogicalProject(__key__=[$9], pageKey=[$9], companyName=['N/A'], profileName=[$2], profileAddress=[$4])
        LogicalFilter(condition=[AND(=($2, $9), =($2, 'Mike'))])
          LogicalJoin(condition=[=($0, $10)], joinType=[inner])
            LogicalTableScan(table=[[testRemoteStore, Profile, $table]])
            LogicalFilter(condition=[=($2, CAST(MyTest($2)):INTEGER)])
              LogicalTableScan(table=[[testavro, PAGEVIEW]])
     */

    assertEquals(1, relRoots.size());
    RelRoot relRoot = relRoots.iterator().next();
    RelNode relNode = relRoot.rel;
    assertTrue(relNode instanceof LogicalProject);
    relNode = relNode.getInput(0);
    assertTrue(relNode instanceof LogicalFilter);
    assertEquals("AND(=($2, $9), =($2, 'Mike'))", ((LogicalFilter) relNode).getCondition().toString());
    relNode = relNode.getInput(0);
    assertTrue(relNode instanceof LogicalJoin);
    assertEquals(2, relNode.getInputs().size());
    LogicalJoin join = (LogicalJoin) relNode;
    RelNode left = join.getLeft();
    RelNode right = join.getRight();
    assertTrue(left instanceof LogicalTableScan);
    assertTrue(right instanceof LogicalFilter);
    assertEquals("=($2, CAST(MyTest($2)):INTEGER)", ((LogicalFilter) right).getCondition().toString());
    assertTrue(right.getInput(0) instanceof LogicalTableScan);
  }

  @Test
  public void testRemoteJoinNoFilterPushDownWithUdfInFilterAndOptimizer() throws SamzaSqlValidatorException {
    Map<String, String> staticConfigs = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(1);

    String sql =
        "Insert into testavro.enrichedPageViewTopic "
            + "select pv.pageKey as __key__, pv.pageKey as pageKey, coalesce(null, 'N/A') as companyName,"
            + "       p.name as profileName, p.address as profileAddress "
            + "from testRemoteStore.Profile.`$table` as p "
            + "join testavro.PAGEVIEW as pv "
            + " on p.__key__ = pv.profileId"
            + " where p.name = pv.pageKey AND p.name = 'Mike' AND pv.profileId = MyTestPoly(p.name)";

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_ENABLE_PLAN_OPTIMIZER, Boolean.toString(true));

    Config samzaConfig = new MapConfig(staticConfigs);
    DslConverter dslConverter = new SamzaSqlDslConverterFactory().create(samzaConfig);
    Collection<RelRoot> relRootsWithOptimization = dslConverter.convertDsl(sql);

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_ENABLE_PLAN_OPTIMIZER, Boolean.toString(false));

    samzaConfig = new MapConfig(staticConfigs);
    dslConverter = new SamzaSqlDslConverterFactory().create(samzaConfig);
    Collection<RelRoot> relRootsWithoutOptimization = dslConverter.convertDsl(sql);

    /*
      LogicalProject(__key__=[$9], pageKey=[$9], companyName=['N/A'], profileName=[$2], profileAddress=[$4])
        LogicalFilter(condition=[AND(=($2, $9), =($2, 'Mike'), =($10, CAST(MyTestPoly($10)):INTEGER))])
          LogicalJoin(condition=[=($0, $10)], joinType=[inner])
            LogicalTableScan(table=[[testRemoteStore, Profile, $table]])
            LogicalTableScan(table=[[testavro, PAGEVIEW]])
     */

    // None of the conditions in the filter could be pushed down as they all require a remote call. Hence the plans
    // with and without optimization should be the same.
    assertEquals(RelOptUtil.toString(relRootsWithOptimization.iterator().next().rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES),
        RelOptUtil.toString(relRootsWithoutOptimization.iterator().next().rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
  }
}

