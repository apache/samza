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
package org.apache.samza.application.internal;

import java.lang.reflect.Constructor;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamAppDescriptor;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.Table;


/**
 * This class implements interface {@link StreamAppDescriptor}. In addition to the common objects for an application
 * defined in {@link AppDescriptorImpl}, this class also includes the high-level DAG {@link StreamGraph} object that user will
 * use to create the processing logic in DAG.
 */
public class StreamAppDescriptorImpl extends AppDescriptorImpl<StreamApplication, StreamAppDescriptor>
    implements StreamAppDescriptor {
  final StreamGraph graph;

  // this config variable is for unit test in samza-api only. *MUST NOT* be set by the user
  private static final String TEST_GRAPH_CLASS_CFG = "app.test.graph.class";
  private static final String DEFAULT_GRAPH_CLASS = "org.apache.samza.operators.StreamGraphSpec";

  public StreamAppDescriptorImpl(StreamApplication userApp, Config config) {
    super(config);
    this.graph = createDefaultGraph(config);
    userApp.describe(this);
  }

  @Override
  public void setDefaultSerde(Serde<?> serde) {
    this.graph.setDefaultSerde(serde);
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId, Serde<M> serde) {
    return this.graph.getInputStream(streamId, serde);
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId) {
    return this.graph.getInputStream(streamId);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId, Serde<M> serde) {
    return this.graph.getOutputStream(streamId, serde);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId) {
    return this.graph.getOutputStream(streamId);
  }

  @Override
  public <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDesc) {
    return this.graph.getTable(tableDesc);
  }

  /**
   * Get the user-defined high-level DAG {@link StreamGraph} object
   *
   * @return the {@link StreamGraph} object defined by the user application
   */
  public StreamGraph getGraph() {
    return graph;
  }

  /**
   * Helper method to load the implementation class of {@link StreamGraph} interface
   *
   * @param config the configuration of the application
   * @return an object implements {@link StreamGraph} interface
   */
  private StreamGraph createDefaultGraph(Config config) {
    String graphClass = config.getOrDefault(TEST_GRAPH_CLASS_CFG, DEFAULT_GRAPH_CLASS);
    try {
      if (StreamGraph.class.isAssignableFrom(Class.forName(graphClass))) {
        Constructor<?> constructor = Class.forName(graphClass).getConstructor(Config.class); // *sigh*
        return (StreamGraph) constructor.newInstance(config);
      } else {
        throw new ConfigException(String.format("Incompatible class %s is invalid. Must implement StreamGraph.", graphClass));
      }
    } catch (Exception e) {
      throw new SamzaException("Cannot instantiate an empty StreamGraph to start user application.", e);
    }
  }

}
