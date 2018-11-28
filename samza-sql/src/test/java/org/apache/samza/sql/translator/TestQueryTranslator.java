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

package org.apache.samza.sql.translator;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.StreamApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.sql.impl.ConfigBasedIOResolverFactory;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.testutil.TestMetricsRegistryImpl;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.sql.testutil.SamzaSqlTestConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.samza.sql.dsl.SamzaSqlDslConverter.fetchQueryInfo;
import static org.apache.samza.sql.dsl.SamzaSqlDslConverter.fetchSqlFromConfig;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;


public class TestQueryTranslator {

  private final Map<String, String> configs = new HashMap<>();
  private final Context mockContext = mock(Context.class);
  private final ContainerContext mockContainerContext = mock(ContainerContext.class);
  private TestMetricsRegistryImpl metricsRegistry = new TestMetricsRegistryImpl();

  @Before
  public void setUp() {
    configs.put("job.default.system", "kafka");
    when(mockContext.getContainerContext()).thenReturn(mockContainerContext);
    when(mockContainerContext.getContainerMetricsRegistry()).thenReturn(metricsRegistry);
  }

  @Test
  public void testTranslate() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
        "Insert into testavro.outputTopic(id) select MyTest(id) from testavro.level1.level2.SIMPLE1 as s where s.id = 10");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl appDesc = new StreamApplicationDescriptorImpl(streamApp -> { },samzaConfig);
    QueryTranslator translator = new QueryTranslator(appDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), appDesc, 0);
    OperatorSpecGraph specGraph = appDesc.getOperatorSpecGraph();

    StreamConfig streamConfig = new StreamConfig(samzaConfig);
    String inputStreamId = specGraph.getInputOperators().keySet().stream().findFirst().get();
    String inputSystem = streamConfig.getSystem(inputStreamId);
    String inputPhysicalName = streamConfig.getPhysicalName(inputStreamId);
    String outputStreamId = specGraph.getOutputStreams().keySet().stream().findFirst().get();
    String outputSystem = streamConfig.getSystem(outputStreamId);
    String outputPhysicalName = streamConfig.getPhysicalName(outputStreamId);

    Assert.assertEquals(1, specGraph.getOutputStreams().size());
    Assert.assertEquals("testavro", outputSystem);
    Assert.assertEquals("outputTopic", outputPhysicalName);
    Assert.assertEquals(1, specGraph.getInputOperators().size());

    Assert.assertEquals("testavro", inputSystem);
    Assert.assertEquals("SIMPLE1", inputPhysicalName);
  }

  @Test
  public void testTranslateComplex() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
        "Insert into testavro.outputTopic(string_value) select Flatten(array_values) from testavro.COMPLEX1");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);

    translator.translate(queryInfo.get(0), streamAppDesc, 0);
    OperatorSpecGraph specGraph = streamAppDesc.getOperatorSpecGraph();

    StreamConfig streamConfig = new StreamConfig(samzaConfig);
    String inputStreamId = specGraph.getInputOperators().keySet().stream().findFirst().get();
    String inputSystem = streamConfig.getSystem(inputStreamId);
    String inputPhysicalName = streamConfig.getPhysicalName(inputStreamId);
    String outputStreamId = specGraph.getOutputStreams().keySet().stream().findFirst().get();
    String outputSystem = streamConfig.getSystem(outputStreamId);
    String outputPhysicalName = streamConfig.getPhysicalName(outputStreamId);

    Assert.assertEquals(1, specGraph.getOutputStreams().size());
    Assert.assertEquals("testavro", outputSystem);
    Assert.assertEquals("outputTopic", outputPhysicalName);
    Assert.assertEquals(1, specGraph.getInputOperators().size());
    Assert.assertEquals("testavro", inputSystem);
    Assert.assertEquals("COMPLEX1", inputPhysicalName);
  }

  @Test
  public void testTranslateSubQuery() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(10);
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT,
        "Insert into testavro.outputTopic(string_value, id) select Flatten(a), id "
            + " from (select id, array_values a, string_value s from testavro.COMPLEX1)");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);

    translator.translate(queryInfo.get(0), streamAppDesc, 0);
    OperatorSpecGraph specGraph = streamAppDesc.getOperatorSpecGraph();

    StreamConfig streamConfig = new StreamConfig(samzaConfig);
    String inputStreamId = specGraph.getInputOperators().keySet().stream().findFirst().get();
    String inputSystem = streamConfig.getSystem(inputStreamId);
    String inputPhysicalName = streamConfig.getPhysicalName(inputStreamId);
    String outputStreamId = specGraph.getOutputStreams().keySet().stream().findFirst().get();
    String outputSystem = streamConfig.getSystem(outputStreamId);
    String outputPhysicalName = streamConfig.getPhysicalName(outputStreamId);

    Assert.assertEquals(1, specGraph.getOutputStreams().size());
    Assert.assertEquals("testavro", outputSystem);
    Assert.assertEquals("outputTopic", outputPhysicalName);
    Assert.assertEquals(1, specGraph.getInputOperators().size());
    Assert.assertEquals("testavro", inputSystem);
    Assert.assertEquals("COMPLEX1", inputPhysicalName);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateStreamTableJoinWithoutJoinOperator() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv, testavro.PROFILE.`$table` as p"
            + " where p.id = pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);

    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateStreamTableJoinWithFullJoinOperator() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv"
            + " full join testavro.PROFILE.`$table` as p"
            + " on p.id = pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);

    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateStreamTableJoinWithSelfJoinOperator() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName)"
            + " select p1.name as profileName"
            + " from testavro.PROFILE.`$table` as p1"
            + " join testavro.PROFILE.`$table` as p2"
            + " on p1.id = p2.id";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);

    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateStreamTableJoinWithThetaCondition() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv"
            + " join testavro.PROFILE.`$table` as p"
            + " on p.id <> pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateStreamTableCrossJoin() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv, testavro.PROFILE.`$table` as p";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateStreamTableJoinWithAndLiteralCondition() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv"
            + " join testavro.PROFILE.`$table` as p"
            + " on p.id = pv.profileId and p.name = 'John'";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateStreamTableJoinWithSubQuery() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv"
            + " where exists "
            + " (select p.id from testavro.PROFILE.`$table` as p"
            + " where p.id = pv.profileId)";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateTableTableJoin() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW.`$table` as pv"
            + " join testavro.PROFILE.`$table` as p"
            + " on p.id = pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateStreamStreamJoin() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv"
            + " join testavro.PROFILE as p"
            + " on p.id = pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateJoinWithIncorrectLeftJoin() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW.`$table` as pv"
            + " left join testavro.PROFILE as p"
            + " on p.id = pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateJoinWithIncorrectRightJoin() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 1);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv"
            + " right join testavro.PROFILE.`$table` as p"
            + " on p.id = pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test (expected = SamzaException.class)
  public void testTranslateStreamTableInnerJoinWithMissingStream() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 10);
    String configIOResolverDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SOURCE_RESOLVER_DOMAIN, "config");
    config.put(configIOResolverDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        ConfigBasedIOResolverFactory.class.getName());
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv"
            + " join testavro.`$table` as p"
            + " on p.id = pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }

  @Test
  public void testTranslateStreamTableInnerJoin() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 10);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv"
            + " join testavro.PROFILE.`$table` as p"
            + " on p.id = pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    config.put(SamzaSqlApplicationConfig.CFG_METADATA_TOPIC_PREFIX, "sampleAppv1");
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);

    translator.translate(queryInfo.get(0), streamAppDesc, 0);
    OperatorSpecGraph specGraph = streamAppDesc.getOperatorSpecGraph();

    StreamConfig streamConfig = new StreamConfig(samzaConfig);
    String input1StreamId = specGraph.getInputOperators().keySet().stream().findFirst().get();
    String input1System = streamConfig.getSystem(input1StreamId);
    String input1PhysicalName = streamConfig.getPhysicalName(input1StreamId);
    String input2StreamId = specGraph.getInputOperators().keySet().stream().skip(1).findFirst().get();
    String input2System = streamConfig.getSystem(input2StreamId);
    String input2PhysicalName = streamConfig.getPhysicalName(input2StreamId);
    String input3StreamId = specGraph.getInputOperators().keySet().stream().skip(2).findFirst().get();
    String input3System = streamConfig.getSystem(input3StreamId);
    String input3PhysicalName = streamConfig.getPhysicalName(input3StreamId);
    String input4StreamId = specGraph.getInputOperators().keySet().stream().skip(3).findFirst().get();
    String input4System = streamConfig.getSystem(input4StreamId);
    String input4PhysicalName = streamConfig.getPhysicalName(input4StreamId);
    String output1StreamId = specGraph.getOutputStreams().keySet().stream().findFirst().get();
    String output1System = streamConfig.getSystem(output1StreamId);
    String output1PhysicalName = streamConfig.getPhysicalName(output1StreamId);
    String output2StreamId = specGraph.getOutputStreams().keySet().stream().skip(1).findFirst().get();
    String output2System = streamConfig.getSystem(output2StreamId);
    String output2PhysicalName = streamConfig.getPhysicalName(output2StreamId);
    String output3StreamId = specGraph.getOutputStreams().keySet().stream().skip(2).findFirst().get();
    String output3System = streamConfig.getSystem(output3StreamId);
    String output3PhysicalName = streamConfig.getPhysicalName(output3StreamId);

    Assert.assertEquals(3, specGraph.getOutputStreams().size());
    Assert.assertEquals("kafka", output1System);
    Assert.assertEquals("sql-job-1-partition_by-sampleAppv1_table_sql_0_join_2", output1PhysicalName);
    Assert.assertEquals("kafka", output2System);
    Assert.assertEquals("sql-job-1-partition_by-sampleAppv1_stream_sql_0_join_2", output2PhysicalName);
    Assert.assertEquals("testavro", output3System);
    Assert.assertEquals("enrichedPageViewTopic", output3PhysicalName);

    Assert.assertEquals(4, specGraph.getInputOperators().size());
    Assert.assertEquals("testavro", input1System);
    Assert.assertEquals("PAGEVIEW", input1PhysicalName);
    Assert.assertEquals("testavro", input2System);
    Assert.assertEquals("PROFILE", input2PhysicalName);
    Assert.assertEquals("kafka", input3System);
    Assert.assertEquals("sql-job-1-partition_by-sampleAppv1_table_sql_0_join_2", input3PhysicalName);
    Assert.assertEquals("kafka", input4System);
    Assert.assertEquals("sql-job-1-partition_by-sampleAppv1_stream_sql_0_join_2", input4PhysicalName);
  }

  @Test
  public void testTranslateStreamTableLeftJoin() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 10);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PAGEVIEW as pv"
            + " left join testavro.PROFILE.`$table` as p"
            + " on p.id = pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);

    translator.translate(queryInfo.get(0), streamAppDesc, 0);

    OperatorSpecGraph specGraph = streamAppDesc.getOperatorSpecGraph();

    StreamConfig streamConfig = new StreamConfig(samzaConfig);
    String input1StreamId = specGraph.getInputOperators().keySet().stream().findFirst().get();
    String input1System = streamConfig.getSystem(input1StreamId);
    String input1PhysicalName = streamConfig.getPhysicalName(input1StreamId);
    String input2StreamId = specGraph.getInputOperators().keySet().stream().skip(1).findFirst().get();
    String input2System = streamConfig.getSystem(input2StreamId);
    String input2PhysicalName = streamConfig.getPhysicalName(input2StreamId);
    String input3StreamId = specGraph.getInputOperators().keySet().stream().skip(2).findFirst().get();
    String input3System = streamConfig.getSystem(input3StreamId);
    String input3PhysicalName = streamConfig.getPhysicalName(input3StreamId);
    String input4StreamId = specGraph.getInputOperators().keySet().stream().skip(3).findFirst().get();
    String input4System = streamConfig.getSystem(input4StreamId);
    String input4PhysicalName = streamConfig.getPhysicalName(input4StreamId);
    String output1StreamId = specGraph.getOutputStreams().keySet().stream().findFirst().get();
    String output1System = streamConfig.getSystem(output1StreamId);
    String output1PhysicalName = streamConfig.getPhysicalName(output1StreamId);
    String output2StreamId = specGraph.getOutputStreams().keySet().stream().skip(1).findFirst().get();
    String output2System = streamConfig.getSystem(output2StreamId);
    String output2PhysicalName = streamConfig.getPhysicalName(output2StreamId);
    String output3StreamId = specGraph.getOutputStreams().keySet().stream().skip(2).findFirst().get();
    String output3System = streamConfig.getSystem(output3StreamId);
    String output3PhysicalName = streamConfig.getPhysicalName(output3StreamId);

    Assert.assertEquals(3, specGraph.getOutputStreams().size());
    Assert.assertEquals("kafka", output1System);
    Assert.assertEquals("sql-job-1-partition_by-table_sql_0_join_2", output1PhysicalName);
    Assert.assertEquals("kafka", output2System);
    Assert.assertEquals("sql-job-1-partition_by-stream_sql_0_join_2", output2PhysicalName);
    Assert.assertEquals("testavro", output3System);
    Assert.assertEquals("enrichedPageViewTopic", output3PhysicalName);

    Assert.assertEquals(4, specGraph.getInputOperators().size());
    Assert.assertEquals("testavro", input1System);
    Assert.assertEquals("PAGEVIEW", input1PhysicalName);
    Assert.assertEquals("testavro", input2System);
    Assert.assertEquals("PROFILE", input2PhysicalName);
    Assert.assertEquals("kafka", input3System);
    Assert.assertEquals("sql-job-1-partition_by-table_sql_0_join_2", input3PhysicalName);
    Assert.assertEquals("kafka", input4System);
    Assert.assertEquals("sql-job-1-partition_by-stream_sql_0_join_2", input4PhysicalName);
  }

  @Test
  public void testTranslateStreamTableRightJoin() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 10);
    String sql =
        "Insert into testavro.enrichedPageViewTopic(profileName, pageKey)"
            + " select p.name as profileName, pv.pageKey"
            + " from testavro.PROFILE.`$table` as p"
            + " right join testavro.PAGEVIEW as pv"
            + " on p.id = pv.profileId";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), streamAppDesc, 0);

    OperatorSpecGraph specGraph = streamAppDesc.getOperatorSpecGraph();

    StreamConfig streamConfig = new StreamConfig(samzaConfig);
    String input1StreamId = specGraph.getInputOperators().keySet().stream().findFirst().get();
    String input1System = streamConfig.getSystem(input1StreamId);
    String input1PhysicalName = streamConfig.getPhysicalName(input1StreamId);
    String input2StreamId = specGraph.getInputOperators().keySet().stream().skip(1).findFirst().get();
    String input2System = streamConfig.getSystem(input2StreamId);
    String input2PhysicalName = streamConfig.getPhysicalName(input2StreamId);
    String input3StreamId = specGraph.getInputOperators().keySet().stream().skip(2).findFirst().get();
    String input3System = streamConfig.getSystem(input3StreamId);
    String input3PhysicalName = streamConfig.getPhysicalName(input3StreamId);
    String input4StreamId = specGraph.getInputOperators().keySet().stream().skip(3).findFirst().get();
    String input4System = streamConfig.getSystem(input4StreamId);
    String input4PhysicalName = streamConfig.getPhysicalName(input4StreamId);
    String output1StreamId = specGraph.getOutputStreams().keySet().stream().findFirst().get();
    String output1System = streamConfig.getSystem(output1StreamId);
    String output1PhysicalName = streamConfig.getPhysicalName(output1StreamId);
    String output2StreamId = specGraph.getOutputStreams().keySet().stream().skip(1).findFirst().get();
    String output2System = streamConfig.getSystem(output2StreamId);
    String output2PhysicalName = streamConfig.getPhysicalName(output2StreamId);
    String output3StreamId = specGraph.getOutputStreams().keySet().stream().skip(2).findFirst().get();
    String output3System = streamConfig.getSystem(output3StreamId);
    String output3PhysicalName = streamConfig.getPhysicalName(output3StreamId);

    Assert.assertEquals(3, specGraph.getOutputStreams().size());
    Assert.assertEquals("kafka", output1System);
    Assert.assertEquals("sql-job-1-partition_by-table_sql_0_join_2", output1PhysicalName);
    Assert.assertEquals("kafka", output2System);
    Assert.assertEquals("sql-job-1-partition_by-stream_sql_0_join_2", output2PhysicalName);
    Assert.assertEquals("testavro", output3System);
    Assert.assertEquals("enrichedPageViewTopic", output3PhysicalName);

    Assert.assertEquals(4, specGraph.getInputOperators().size());
    Assert.assertEquals("testavro", input1System);
    Assert.assertEquals("PROFILE", input1PhysicalName);
    Assert.assertEquals("testavro", input2System);
    Assert.assertEquals("PAGEVIEW", input2PhysicalName);
    Assert.assertEquals("kafka", input3System);
    Assert.assertEquals("sql-job-1-partition_by-table_sql_0_join_2", input3PhysicalName);
    Assert.assertEquals("kafka", input4System);
    Assert.assertEquals("sql-job-1-partition_by-stream_sql_0_join_2", input4PhysicalName);
  }

  @Test
  public void testTranslateGroupBy() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 10);
    String sql =
        "Insert into testavro.pageViewCountTopic(jobName, pageKey, `count`)"
            + " select 'SampleJob' as jobName, pv.pageKey, count(*) as `count`"
            + " from testavro.PAGEVIEW as pv"
            + " where pv.pageKey = 'job' or pv.pageKey = 'inbox'"
            + " group by (pv.pageKey)";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);

    translator.translate(queryInfo.get(0), streamAppDesc, 0);
    OperatorSpecGraph specGraph = streamAppDesc.getOperatorSpecGraph();

    Assert.assertEquals(1, specGraph.getInputOperators().size());
    Assert.assertEquals(1, specGraph.getOutputStreams().size());
    assertTrue(specGraph.hasWindowOrJoins());
    Collection<OperatorSpec> operatorSpecs = specGraph.getAllOperatorSpecs();
  }

  @Test (expected = SamzaException.class)
  public void testTranslateGroupByWithSumAggregator() {
    Map<String, String> config = SamzaSqlTestConfig.fetchStaticConfigsWithFactories(configs, 10);
    String sql =
        "Insert into testavro.pageViewCountTopic(jobName, pageKey, `sum`)"
            + " select 'SampleJob' as jobName, pv.pageKey, sum(pv.profileId) as `sum`"
            + " from testavro.PAGEVIEW as pv" + " where pv.pageKey = 'job' or pv.pageKey = 'inbox'"
            + " group by (pv.pageKey)";
    config.put(SamzaSqlApplicationConfig.CFG_SQL_STMT, sql);
    Config samzaConfig = SamzaSqlApplicationRunner.computeSamzaConfigs(true, new MapConfig(config));

    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig samzaSqlApplicationConfig = new SamzaSqlApplicationConfig(new MapConfig(config),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    StreamApplicationDescriptorImpl streamAppDesc = new StreamApplicationDescriptorImpl(streamApp -> { }, samzaConfig);
    QueryTranslator translator = new QueryTranslator(streamAppDesc, samzaSqlApplicationConfig);
    translator.translate(queryInfo.get(0), streamAppDesc, 0);
  }
}
