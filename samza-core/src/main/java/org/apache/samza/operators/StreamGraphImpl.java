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
import org.apache.samza.system.StreamSpec;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The {@link StreamGraph} implementation that provides APIs for accessing {@link MessageStream}s to be used to
 * create the DAG of transforms.
 */
public class StreamGraphImpl implements StreamGraph {

  /**
   * Unique identifier for each {@link org.apache.samza.operators.spec.OperatorSpec} in the graph.
   * Should only be accessed by {@link MessageStreamImpl} via {@link #getNextOpId()}.
   */
  private int opId = 0;

  private final Map<StreamSpec, InputStream> inStreams = new HashMap<>();
  private final Map<StreamSpec, OutputStream> outStreams = new HashMap<>();
  private final ApplicationRunner runner;
  private final Config config;

  private ContextManager contextManager = new ContextManager() { };

  public StreamGraphImpl(ApplicationRunner runner, Config config) {
    // TODO: SAMZA-1118 - Move StreamSpec and ApplicationRunner out of StreamGraphImpl once Systems
    // can use streamId to send and receive messages.
    this.runner = runner;
    this.config = config;
  }

  @Override
  public <K, V, M> MessageStream<M> getInputStream(String streamId, BiFunction<K, V, M> msgBuilder) {
    return inStreams.computeIfAbsent(runner.getStreamSpec(streamId),
        streamSpec -> new InputStreamImpl<>(this, streamSpec, msgBuilder));
  }

  @Override
  public StreamGraph withContextManager(ContextManager contextManager) {
    this.contextManager = contextManager;
    return this;
  }

  /**
   * Internal helper for {@link MessageStreamImpl} to add an output {@link MessageStream} to the graph.
   *
   * @param streamId the unique logical ID for the stream
   * @param keyExtractor the {@link Function} to extract the outgoing key from the output message
   * @param msgExtractor the {@link Function} to extract the outgoing message from the output message
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param <M> the type of message in the output {@link MessageStream}
   * @return the output {@link MessageStream}
   */
  <K, V, M> MessageStream<M> getOutputStream(String streamId,
      Function<M, K> keyExtractor, Function<M, V> msgExtractor) {
    return outStreams.computeIfAbsent(runner.getStreamSpec(streamId),
        streamSpec -> new OutputStreamImpl<>(this, streamSpec, keyExtractor, msgExtractor));
  }

  /**
   * Internal helper for {@link MessageStreamImpl} to add an intermediate {@link MessageStream} to the graph.
   * An intermediate {@link MessageStream} is both an output and an input stream.
   *
   * @param streamName the name of the stream to be created. Will be prefixed with job name and id to generate the
   *                   logical streamId.
   * @param keyExtractor the {@link Function} to extract the outgoing key from the intermediate message
   * @param msgExtractor the {@link Function} to extract the outgoing message from the intermediate message
   * @param msgBuilder the {@link BiFunction} to convert the incoming key and message to a message
   *                   in the intermediate {@link MessageStream}
   * @param <K> the type of key in the intermediate message
   * @param <V> the type of message in the intermediate message
   * @param <M> the type of messages in the intermediate {@link MessageStream}
   * @return  the intermediate {@link MessageStreamImpl}
   */
  <K, V, M> MessageStreamImpl<M> getIntermediateStream(String streamName,
      Function<M, K> keyExtractor, Function<M, V> msgExtractor, BiFunction<K, V, M> msgBuilder) {
    String streamId = String.format("%s-%s-%s",
        config.get(JobConfig.JOB_NAME()),
        config.get(JobConfig.JOB_ID(), "1"),
        streamName);
    StreamSpec streamSpec = runner.getStreamSpec(streamId);
    IntermediateStreamImpl<K, V, M> intStream = (IntermediateStreamImpl<K, V, M>) inStreams.computeIfAbsent(
        streamSpec, k -> new IntermediateStreamImpl<>(this, streamSpec,
            keyExtractor, msgExtractor, msgBuilder));
    outStreams.putIfAbsent(streamSpec, intStream);
    return intStream;
  }

  public Map<StreamSpec, InputStream> getInputStreams() {
    return Collections.unmodifiableMap(inStreams);
  }

  public Map<StreamSpec, OutputStream> getOutputStreams() {
    return Collections.unmodifiableMap(outStreams);
  }

  public ContextManager getContextManager() {
    return this.contextManager;
  }

  int getNextOpId() {
    return this.opId++;
  }
}
