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

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.task.TaskContext;


/**
 * This class is used to populate the {@link StreamApplicationDescriptor} using the SQL queries.
 * This class contains the core of the SamzaSQL control code that converts the SQL statements to calcite relational graph.
 * It then walks the relational graph and then populates the Samza's {@link StreamApplicationDescriptor} accordingly.
 */
public class QueryTranslator {
  private final ScanTranslator scanTranslator;
  private final ModifyTranslator modifyTranslator;
  private final SamzaSqlApplicationConfig sqlConfig;
  private final Map<String, SamzaRelConverter> converters;

  public QueryTranslator(SamzaSqlApplicationConfig sqlConfig) {
    this.sqlConfig = sqlConfig;
    scanTranslator =
        new ScanTranslator(sqlConfig.getSamzaRelConverters(), sqlConfig.getInputSystemStreamConfigBySource());
    modifyTranslator =
        new ModifyTranslator(sqlConfig.getSamzaRelConverters(), sqlConfig.getOutputSystemStreamConfigsBySource());
    this.converters = sqlConfig.getSamzaRelConverters();
  }

  public void translate(SamzaSqlQueryParser.QueryInfo queryInfo, StreamApplicationDescriptor appDesc) {
    QueryPlanner planner =
        new QueryPlanner(sqlConfig.getRelSchemaProviders(), sqlConfig.getSystemStreamConfigsBySource(),
            sqlConfig.getUdfMetadata());
    final RelRoot relRoot = planner.plan(queryInfo.getSql());
    translate(relRoot, appDesc);
  }

  public void translate(RelRoot relRoot, StreamApplicationDescriptor appDesc) {
    final SamzaSqlExecutionContext executionContext = new SamzaSqlExecutionContext(this.sqlConfig);
    final TranslatorContext context = new TranslatorContext(appDesc, relRoot, executionContext, this.converters);
    final SqlIOResolver ioResolver = context.getExecutionContext().getSamzaSqlApplicationConfig().getIoResolver();
    final RelNode node = relRoot.project();

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

    appDesc.withContextManager(new ContextManager() {
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
