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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.descriptors.GenericSystemDescriptor;
import org.apache.samza.sql.data.RexToJavaCompiler;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.interfaces.SamzaRelConverter;


/**
 * State that is maintained while translating the Calcite relational graph to Samza {@link StreamGraph}.
 */
public class TranslatorContext implements Cloneable {
  /**
   * The internal variables that are shared among all cloned {@link TranslatorContext}
   */
  private final StreamGraph streamGraph;
  private final RexToJavaCompiler compiler;
  private final Map<String, SamzaRelConverter> relSamzaConverters;
  private final Map<Integer, MessageStream> messageStreams;
  private final Map<Integer, RelNode> relNodes;
  private final Map<String, GenericSystemDescriptor> systemDescriptors;

  /**
   * The internal variables that are not shared among all cloned {@link TranslatorContext}
   */
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

  private RexToJavaCompiler createExpressionCompiler(RelRoot relRoot) {
    RelDataTypeFactory dataTypeFactory = relRoot.project().getCluster().getTypeFactory();
    RexBuilder rexBuilder = new SamzaSqlRexBuilder(dataTypeFactory);
    return new RexToJavaCompiler(rexBuilder);
  }

  /**
   * Private constructor to make a clone of {@link TranslatorContext} object
   *
   * @param other the original object to copy from
   */
  private TranslatorContext(TranslatorContext other) {
    this.streamGraph  = other.streamGraph;
    this.compiler = other.compiler;
    this.relSamzaConverters = other.relSamzaConverters;
    this.messageStreams = other.messageStreams;
    this.relNodes = other.relNodes;
    this.executionContext = other.executionContext.clone();
    this.dataContext = new DataContextImpl();
    this.systemDescriptors = other.systemDescriptors;
  }

  /**
   * Create the instance of TranslatorContext
   * @param streamGraph Samza's streamGraph that is populated during the translation.
   * @param relRoot Root of the relational graph from calcite.
   * @param executionContext the execution context
   * @param converters the map of schema to RelData converters
   */
  TranslatorContext(StreamGraph streamGraph, RelRoot relRoot, SamzaSqlExecutionContext executionContext, Map<String, SamzaRelConverter> converters) {
    this.streamGraph = streamGraph;
    this.compiler = createExpressionCompiler(relRoot);
    this.executionContext = executionContext;
    this.dataContext = new DataContextImpl();
    this.relSamzaConverters = converters;
    this.messageStreams = new HashMap<>();
    this.relNodes = new HashMap<>();
    this.systemDescriptors = new HashMap<>();
  }

  /**
   * Gets stream graph.
   *
   * @return the stream graph
   */
  public StreamGraph getStreamGraph() {
    return streamGraph;
  }

  /**
   * Gets execution context.
   *
   * @return the execution context
   */
  SamzaSqlExecutionContext getExecutionContext() {
    return executionContext;
  }

  DataContext getDataContext() {
    return dataContext;
  }

  /**
   * Gets expression compiler.
   *
   * @return the expression compiler
   */
  RexToJavaCompiler getExpressionCompiler() {
    return compiler;
  }

  /**
   * Register message stream.
   *
   * @param id the id
   * @param stream the stream
   */
  void registerMessageStream(int id, MessageStream stream) {
    messageStreams.put(id, stream);
  }

  /**
   * Gets message stream.
   *
   * @param id the id
   * @return the message stream
   */
  MessageStream getMessageStream(int id) {
    return messageStreams.get(id);
  }

  void registerRelNode(int id, RelNode relNode) {
    relNodes.put(id, relNode);
  }

  RelNode getRelNode(int id) {
    return relNodes.get(id);
  }

  SamzaRelConverter getMsgConverter(String source) {
    return this.relSamzaConverters.get(source);
  }

  Map<String, GenericSystemDescriptor> getSystemDescriptors() {
    return this.systemDescriptors;
  }

  /**
   * This method helps to create a per task instance of translator context
   *
   * @return the cloned instance of {@link TranslatorContext}
   */
  @Override
  public TranslatorContext clone() {
    return new TranslatorContext(this);
  }
}
