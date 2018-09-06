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

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is used to populate the StreamGraph using the SQL queries.
 * This class contains the core of the SamzaSQL control code that converts the SQL statements to calcite relational graph.
 * It then walks the relational graph and then populates the Samza's {@link StreamGraph} accordingly.
 */
public class QueryTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(QueryTranslator.class);

  private final ScanTranslator scanTranslator;
  private final ModifyTranslator modifyTranslator;
  private final SamzaSqlApplicationConfig sqlConfig;
  private final Map<String, SamzaRelConverter> converters;

  private final Set<String> inputSystemStreams = new HashSet<>();
  private final Set<String> outputSystemStreams = new HashSet<>();
  private final Set<String> systemStreams = new HashSet<>();

  public QueryTranslator(SamzaSqlApplicationConfig sqlConfig) {
    this.sqlConfig = sqlConfig;
    scanTranslator =
        new ScanTranslator(sqlConfig.getSamzaRelConverters(), sqlConfig.getInputSystemStreamConfigBySource());
    modifyTranslator =
        new ModifyTranslator(sqlConfig.getSamzaRelConverters(), sqlConfig.getOutputSystemStreamConfigsBySource());
    this.converters = sqlConfig.getSamzaRelConverters();
  }

  // package private: for internal tests
  void translate(SamzaSqlQueryParser.QueryInfo queryInfo, StreamGraph streamGraph) {
    QueryPlanner planner =
        new QueryPlanner(sqlConfig.getRelSchemaProviders(), sqlConfig.getSystemStreamConfigsBySource(),
            sqlConfig.getUdfMetadata());
    final RelRoot relRoot = planner.plan(queryInfo.getSql());

    populateSystemStreams(relRoot);
    systemStreams.addAll(inputSystemStreams);
    systemStreams.addAll(outputSystemStreams);

    translate(relRoot, streamGraph);
  }

  private void populateSystemStreams(RelRoot relRoot) {
    populateSystemStreams(relRoot.project());
  }

  private void populateSystemStreams(RelNode relNode) {
    if (relNode instanceof TableModify) {
      outputSystemStreams.add(getSystemStreamName(relNode));
    } else {
      if (relNode instanceof BiRel) {
        BiRel biRelNode = (BiRel) relNode;
        String leftSystemStream = getSystemStreamName(biRelNode.getLeft());
        if (leftSystemStream != null) {
          inputSystemStreams.add(leftSystemStream);
        }
        String rightSystemStream = getSystemStreamName(biRelNode.getRight());
        if (leftSystemStream != null) {
          inputSystemStreams.add(rightSystemStream);
        }
      } else {
        if (relNode.getTable() != null) {
          inputSystemStreams.add(getSystemStreamName(relNode));
        }
      }
    }

    List<RelNode> relNodes = relNode.getInputs();
    if (relNodes == null || relNodes.isEmpty()) {
      return;
    }
    relNodes.forEach(this::populateSystemStreams);
  }

  private String getSystemStreamName(RelNode relNode) {
    RelOptTable table = relNode.getTable();
    // Table could be null for joins. For instance, a join on Udfs
    if (table == null) {
      List<RelNode> relNodes = relNode.getInputs();
      if (relNodes == null || relNodes.isEmpty()) {
        return null;
      }
      relNodes.forEach(this::populateSystemStreams);
      return null;
    }
    return table.getQualifiedName().stream().map(Object::toString).collect(Collectors.joining("."));
  }

  public void translate(RelRoot relRoot, StreamGraph streamGraph) {
    final SamzaSqlExecutionContext executionContext = new SamzaSqlExecutionContext(this.sqlConfig);
    final TranslatorContext context = new TranslatorContext(streamGraph, relRoot, executionContext, this.converters);
    final SqlIOResolver ioResolver = context.getExecutionContext().getSamzaSqlApplicationConfig().getIoResolver();
    RelNode node = relRoot.project();

    node.accept(new RelShuttleImpl() {
      int windowId = 0;
      int joinId = 0;

      @Override
      public RelNode visit(RelNode relNode) {
        if (relNode instanceof TableModify) {
          return visit((TableModify) relNode);
        }
        return super.visit(relNode);
      }

      private RelNode visit(TableModify modify) {
        if (!modify.isInsert()) {
          throw new SamzaException("Not a supported operation: " + modify.toString());
        }
        RelNode node = super.visit(modify);
        modifyTranslator.translate(modify, context);
        return node;
      }

      @Override
      public RelNode visit(TableScan scan) {
        RelNode node = super.visit(scan);
        scanTranslator.translate(scan, context);
        return node;
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        RelNode node = visitChild(filter, 0, filter.getInput());
        new FilterTranslator().translate(filter, context);
        return node;
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode node = super.visit(project);
        new ProjectTranslator().translate(project, context);
        return node;
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode node = super.visit(join);
        joinId++;
        new JoinTranslator(joinId, ioResolver).translate(join, context);
        return node;
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        RelNode node = super.visit(aggregate);
        windowId++;
        new LogicalAggregateTranslator(windowId).translate(aggregate, context);
        return node;
      }
    });

    streamGraph.withContextManager(new ContextManager() {
      @Override
      public void init(Config config, TaskContext taskContext) {
        taskContext.setUserContext(context.clone());
      }

      @Override
      public void close() {

      }

    });

  }
}
