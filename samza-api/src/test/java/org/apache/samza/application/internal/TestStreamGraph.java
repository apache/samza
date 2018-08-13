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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.Table;

import static org.mockito.Mockito.*;


/**
 * Test implementation of {@link StreamGraph} used only in unit test
 */
public class TestStreamGraph implements StreamGraph {
  final Config config;
  final List<String> inputStreams = new ArrayList<>();
  final List<String> outputStreams = new ArrayList<>();
  final List<TableDescriptor> tables = new ArrayList<>();
  final Map<String, Serde> inputSerdes = new HashMap<>();
  final Map<String, Serde> outputSerdes = new HashMap<>();
  Serde defaultSerde;

  public TestStreamGraph(Config config) {
    this.config = config;
  }

  @Override
  public void setDefaultSerde(Serde<?> serde) {
    this.defaultSerde = serde;
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId, Serde<M> serde) {
    this.inputStreams.add(streamId);
    this.inputSerdes.put(streamId, serde);
    return mock(MessageStream.class);
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId) {
    this.inputStreams.add(streamId);
    return mock(MessageStream.class);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId, Serde<M> serde) {
    this.outputStreams.add(streamId);
    this.outputSerdes.put(streamId, serde);
    return mock(OutputStream.class);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId) {
    this.outputStreams.add(streamId);
    return mock(OutputStream.class);
  }

  @Override
  public <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDesc) {
    this.tables.add(tableDesc);
    return mock(Table.class);
  }
}
