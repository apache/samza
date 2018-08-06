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
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationSpec;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.Table;


/**
 * This class implements interface {@link StreamApplicationSpec}. In addition to the common objects for an application
 * defined in {@link AppSpecImpl}, this class also includes the high-level DAG {@link StreamGraph} object that user will
 * use to create the processing logic in DAG.
 */
public class StreamAppSpecImpl extends AppSpecImpl<StreamApplication, StreamApplicationSpec> implements StreamApplicationSpec {
  final StreamGraph graph;

  public StreamAppSpecImpl(StreamApplication userApp, Config config) {
    super(config);
    this.graph = createDefaultGraph(config);
    userApp.describe(this);
  }

  private StreamGraph createDefaultGraph(Config config) {
    try {
      Constructor<?> constructor = Class.forName("org.apache.samza.operators.StreamGraphSpec").getConstructor(Config.class); // *sigh*
      return (StreamGraph) constructor.newInstance(config);
    } catch (Exception e) {
      throw new SamzaException("Cannot instantiate an empty StreamGraph to start user application.", e);
    }
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

}
