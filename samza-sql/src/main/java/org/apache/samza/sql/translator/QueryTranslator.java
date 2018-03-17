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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SqlSystemStreamConfig;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.table.Table;
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
  private final SamzaSqlApplicationConfig sqlConfig;

  public QueryTranslator(SamzaSqlApplicationConfig sqlConfig) {
    this.sqlConfig = sqlConfig;
    scanTranslator =
        new ScanTranslator(sqlConfig.getSamzaRelConverters(), sqlConfig.getInputSystemStreamConfigBySource());
  }

  public void translate(SamzaSqlQueryParser.QueryInfo queryInfo, StreamGraph streamGraph) {
    QueryPlanner planner =
        new QueryPlanner(sqlConfig.getRelSchemaProviders(), sqlConfig.getInputSystemStreamConfigBySource(),
            sqlConfig.getUdfMetadata());
    final SamzaSqlExecutionContext executionContext = new SamzaSqlExecutionContext(this.sqlConfig);
    final RelRoot relRoot = planner.plan(queryInfo.getSelectQuery());
    final TranslatorContext context = new TranslatorContext(streamGraph, relRoot, executionContext);
    final RelNode node = relRoot.project();

    node.accept(new RelShuttleImpl() {
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
    });

    SqlSystemStreamConfig outputSystemConfig =
        sqlConfig.getOutputSystemStreamConfigsBySource().get(queryInfo.getOutputSource());
    SamzaRelConverter samzaMsgConverter = sqlConfig.getSamzaRelConverters().get(queryInfo.getOutputSource());
    MessageStreamImpl<SamzaSqlRelMessage> stream = (MessageStreamImpl<SamzaSqlRelMessage>) context.getMessageStream(node.getId());
    MessageStream<KV<Object, Object>> outputStream = stream.map(samzaMsgConverter::convertToSamzaMessage);

    if (!outputSystemConfig.isTable()) {
      outputStream.sendTo(streamGraph.getOutputStream(outputSystemConfig.getStreamName()));
    } else {
      Table outputTable = streamGraph.getTable(outputSystemConfig.getTableDescriptor());
      if (outputTable == null) {
        String msg = "Failed to obtain table with " + outputSystemConfig.getSource();
        LOG.error(msg);
        throw new SamzaException(msg);
      }
      outputStream.sendTo(outputTable);
    }
  }
}
