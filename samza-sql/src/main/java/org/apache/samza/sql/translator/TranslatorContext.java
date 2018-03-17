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

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.sql.data.RexToJavaCompiler;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.table.Table;


/**
 * State that is maintained while translating the Calcite relational graph to Samza {@link StreamGraph}.
 */
public class TranslatorContext {
  private final StreamGraph streamGraph;
  private final Map<Integer, MessageStream> messsageStreams = new HashMap<>();
  private final Map<String, Table> tables = new HashMap<>();
  private final RexToJavaCompiler compiler;

  private final SamzaSqlExecutionContext executionContext;
  private final DataContextImpl dataContext;

  private static class DataContextImpl implements DataContext {

    @Override
    public SchemaPlus getRootSchema() {
      return null;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
      return null;
    }

    @Override
    public QueryProvider getQueryProvider() {
      return null;
    }

    @Override
    public Object get(String name) {
      if (name.equals(Variable.CURRENT_TIMESTAMP.camelName)) {
        return System.currentTimeMillis();
      }

      return null;
    }
  }

  private static class SamzaSqlRexBuilder extends RexBuilder {
    private SamzaSqlRexBuilder(RelDataTypeFactory typeFactory) {
      super(typeFactory);
    }

    /**
     * Since Drill has different mechanism and rules for implicit casting,
     * ensureType() is overridden to avoid conflicting cast functions being added to the expressions.
     */
    @Override
    public RexNode ensureType(RelDataType type, RexNode node, boolean matchNullability) {
      return node;
    }
  }

  /**
   * Create the instance of TranslatorContext
   * @param streamGraph Samza's streamGraph that is populated during the translation.
   * @param relRoot Root of the relational graph from calcite.
   * @param executionContext the execution context
   */
  public TranslatorContext(StreamGraph streamGraph, RelRoot relRoot, SamzaSqlExecutionContext executionContext) {
    this.streamGraph = streamGraph;
    this.compiler = createExpressionCompiler(relRoot);
    this.executionContext = executionContext;
    this.dataContext = new DataContextImpl();
  }

  /**
   * Gets stream graph.
   *
   * @return the stream graph
   */
  public StreamGraph getStreamGraph() {
    return streamGraph;
  }

  private RexToJavaCompiler createExpressionCompiler(RelRoot relRoot) {
    RelDataTypeFactory dataTypeFactory = relRoot.project().getCluster().getTypeFactory();
    RexBuilder rexBuilder = new SamzaSqlRexBuilder(dataTypeFactory);
    return new RexToJavaCompiler(rexBuilder);
  }

  /**
   * Gets execution context.
   *
   * @return the execution context
   */
  public SamzaSqlExecutionContext getExecutionContext() {
    return executionContext;
  }

  public DataContext getDataContext() {
    return dataContext;
  }

  /**
   * Gets expression compiler.
   *
   * @return the expression compiler
   */
  public RexToJavaCompiler getExpressionCompiler() {
    return compiler;
  }

  /**
   * Register message stream.
   *
   * @param id the id
   * @param stream the stream
   */
  public void registerMessageStream(int id, MessageStream stream) {
    messsageStreams.put(id, stream);
  }

  /**
   * Gets message stream.
   *
   * @param id the id
   * @return the message stream
   */
  public MessageStream getMessageStream(int id) {
    return messsageStreams.get(id);
  }
}
