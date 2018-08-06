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
package org.apache.samza.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.Table;

import static org.mockito.Mockito.mock;


/**
 * Test class only to enable loading a fake impl of {@link StreamGraphSpec} for unit test in samza-api module. The real
 * implementation of {@link StreamGraphSpec} is in samza-core module.
 */
public class StreamGraphSpec implements StreamGraph {
  public final Config config;
  public Serde defaultSerde;
  public final List<String> inputStreams = new ArrayList<>();
  public final Map<String, Serde> inputSerdes = new HashMap<>();
  public final List<String> outputStreams = new ArrayList<>();
  public final Map<String, Serde> outputSerdes = new HashMap<>();
  public final List<TableDescriptor> tables = new ArrayList<>();

  public StreamGraphSpec(Config config) {
    this.config = config;
  }

  @Override
  public void setDefaultSerde(Serde<?> serde) {
    this.defaultSerde = serde;
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId, Serde<M> serde) {
    inputStreams.add(streamId);
    inputSerdes.put(streamId, serde);
    return mock(MessageStream.class);
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId) {
    inputStreams.add(streamId);
    return mock(MessageStream.class);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId, Serde<M> serde) {
    outputStreams.add(streamId);
    outputSerdes.put(streamId, serde);
    return mock(OutputStream.class);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId) {
    outputStreams.add(streamId);
    return mock(OutputStream.class);
  }

  @Override
  public <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDesc) {
    tables.add(tableDesc);
    return mock(Table.class);
  }
}
