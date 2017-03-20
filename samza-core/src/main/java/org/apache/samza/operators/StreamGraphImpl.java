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

import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.stream.InputStream;
import org.apache.samza.operators.stream.InputStreamImpl;
import org.apache.samza.operators.stream.IntermediateStreamImpl;
import org.apache.samza.operators.stream.OutputStream;
import org.apache.samza.operators.stream.OutputStreamImpl;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The {@link StreamGraph} implementation that allows users to create input and output system streams.
 */
public class StreamGraphImpl implements StreamGraph {
  /**
   * Unique identifier for each {@link org.apache.samza.operators.spec.OperatorSpec} in the graph.
   * Should only be accessed via {@link #getNextOpId()}.
   */
  private int opId = 0;

  private final Map<String, InputStream> inStreams = new HashMap<>();
  private final Map<String, OutputStream> outStreams = new HashMap<>();
  private final ApplicationRunner runner;
  private final Config config;

  private ContextManager contextManager = new ContextManager() { };

  public StreamGraphImpl(ApplicationRunner runner, Config config) {
    this.runner = runner;
    this.config = config;
  }

  @Override
  public <K, V, M> MessageStream<M> createInStream(StreamSpec streamSpec,
      BiFunction<K, V, M> msgBuilder, Serde<K> keySerde, Serde<V> msgSerde) {
    return inStreams.computeIfAbsent(streamSpec.getId(),
        streamId -> new InputStreamImpl<>(this, streamSpec, msgBuilder, keySerde, msgSerde));
  }

  @Override
  public <K, V, M> MessageStream<M> createOutStream(StreamSpec streamSpec,
      Function<M, K> keyExtractor, Function<M, V> msgExtractor,
      Serde<K> keySerde, Serde<V> msgSerde) {
    return outStreams.computeIfAbsent(streamSpec.getId(),
        streamId -> new OutputStreamImpl<>(this, streamSpec, keyExtractor, msgExtractor, keySerde, msgSerde));
  }

  @Override
  public StreamGraph withContextManager(ContextManager manager) {
    this.contextManager = manager;
    return this;
  }

  /**
   * Internal helper method to create an intermediate stream for an operator.
   *
   * @param streamName the name of the stream to be created (will be prefixed with job name and id)
   * @param keyExtractor the function to extract the output key from the input message
   * @param msgExtractor the function to extract the output message from the input message
   * @param msgBuilder the function to convert the incoming key and message to the output message
   * @param <K> the type of key in the incoming message
   * @param <V> the type of message in the incoming message
   * @param <M> the type of messages in the intermediate {@link MessageStream}
   * @return  the output {@link MessageStreamImpl} for the re-partitioned stream
   */
  <K, V, M> MessageStreamImpl<M> createIntStream(String streamName, Function<M, K> keyExtractor, Function<M, V> msgExtractor,
      BiFunction<K, V, M> msgBuilder) {
    String streamId = String.format("%s-%s-%s",
        config.get(JobConfig.JOB_NAME()),
        config.get(JobConfig.JOB_ID(), "1"),
        streamName);
    StreamSpec streamSpec = runner.getStreamSpec(streamId);

    // intermediate streams use default system serdes
    return createIntStream(streamSpec, keyExtractor, msgExtractor, msgBuilder, null, null);
  }

  /**
   * Internal helper method to create and add an intermediate {@link MessageStream} to the graph.
   *
   * @param streamSpec  the {@link StreamSpec} describing the physical characteristics of the intermediate {@link MessageStream}
   * @param keyExtractor the function to extract the output key from the input message
   * @param msgExtractor the function to extract the output message from the input message
   * @param msgBuilder the function to convert the incoming key and message to the outgoing message of type {@code M}
   * @param keySerde  the serde used to serialize/deserialize the message key from the intermediate {@link MessageStream}
   * @param msgSerde  the serde used to serialize/deserialize the message body from the intermediate {@link MessageStream}
   * @param <K> the type of key in the incoming message
   * @param <V> the type of message in the incoming message
   * @param <M> the type of messages in the intermediate {@link MessageStream}
   * @return the intermediate {@link MessageStream} object
   */
  <K, V, M> MessageStreamImpl<M> createIntStream(StreamSpec streamSpec,
      Function<M, K> keyExtractor, Function<M, V> msgExtractor, BiFunction<K, V, M> msgBuilder,
      Serde<K> keySerde, Serde<V> msgSerde) {
    IntermediateStreamImpl<K, V, M> intStream = (IntermediateStreamImpl<K, V, M>) inStreams.computeIfAbsent(
        streamSpec.getId(), streamId -> new IntermediateStreamImpl<>(this, streamSpec,
            keyExtractor, msgExtractor, msgBuilder, keySerde, msgSerde));
    outStreams.putIfAbsent(streamSpec.getId(), intStream);
    return intStream;
  }

  public Map<StreamSpec, MessageStream> getInStreams() {
    Map<StreamSpec, MessageStream> inStreamMap = new HashMap<>();
    this.inStreams.forEach((ss, entry) -> inStreamMap.put(entry.getStreamSpec(), entry));
    return inStreamMap;
  }

  public Map<StreamSpec, MessageStream> getOutStreams() {
    Map<StreamSpec, MessageStream> outStreamMap = new HashMap<>();
    this.outStreams.forEach((ss, entry) -> outStreamMap.put(entry.getStreamSpec(), entry));
    return outStreamMap;
  }

  public ContextManager getContextManager() {
    return this.contextManager;
  }

  public int getNextOpId() {
    return this.opId++;
  }

  /**
   * Get the input stream corresponding to a {@link SystemStream}
   *
   * @param sStream the {@link SystemStream}
   * @return the {@link MessageStreamImpl} corresponding to the {@code systemStream}, or null
   */
  public MessageStreamImpl getInputStream(SystemStream sStream) {
    for (InputStream inStream: this.inStreams.values()) {
      StreamSpec inStreamSpec = inStream.getStreamSpec();
      if (inStreamSpec.getSystemName().equals(sStream.getSystem()) &&
          inStreamSpec.getPhysicalName().equals(sStream.getStream())) {
        return (MessageStreamImpl) inStream;
      }
    }
    return null;
  }
}
