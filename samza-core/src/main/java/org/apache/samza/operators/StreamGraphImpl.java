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
import org.apache.samza.operators.stream.InputStreamInternal;
import org.apache.samza.operators.stream.InputStreamInternalImpl;
import org.apache.samza.operators.stream.IntermediateStreamInternalImpl;
import org.apache.samza.operators.stream.OutputStreamInternal;
import org.apache.samza.operators.stream.OutputStreamInternalImpl;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.StreamSpec;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A {@link StreamGraph} that provides APIs for accessing {@link MessageStream}s to be used to
 * create the DAG of transforms.
 */
public class StreamGraphImpl implements StreamGraph {

  /**
   * Unique identifier for each {@link org.apache.samza.operators.spec.OperatorSpec} in the graph.
   * Should only be accessed by {@link MessageStreamImpl} via {@link #getNextOpId()}.
   */
  private int opId = 0;

  private final Map<StreamSpec, InputStreamInternal> inStreams = new HashMap<>();
  private final Map<StreamSpec, OutputStreamInternal> outStreams = new HashMap<>();
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
  public <K, V, M> MessageStream<M> getInputStream(String streamId, BiFunction<? super K, ? super V, ? extends M> msgBuilder) {
    if (msgBuilder == null) {
      throw new IllegalArgumentException("msgBuilder can't be null for an input stream");
    }

    if (inStreams.containsKey(runner.getStreamSpec(streamId))) {
      throw new IllegalStateException("Cannot invoke getInputStream() multiple times with the same streamId: " + streamId);
    }

    return inStreams.computeIfAbsent(runner.getStreamSpec(streamId),
        streamSpec -> new InputStreamInternalImpl<>(this, streamSpec, (BiFunction<K, V, M>) msgBuilder));
  }

  @Override
  public <K, V, M> OutputStream<K, V, M> getOutputStream(String streamId,
      Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor) {
    if (keyExtractor == null) {
      throw new IllegalArgumentException("keyExtractor can't be null for an output stream.");
    }

    if (msgExtractor == null) {
      throw new IllegalArgumentException("msgExtractor can't be null for an output stream.");
    }

    if (outStreams.containsKey(runner.getStreamSpec(streamId))) {
      throw new IllegalStateException("Cannot invoke getOutputStream() multiple times with the same streamId: " + streamId);
    }

    return outStreams.computeIfAbsent(runner.getStreamSpec(streamId),
        streamSpec -> new OutputStreamInternalImpl<>(this, streamSpec, (Function<M, K>) keyExtractor, (Function<M, V>) msgExtractor));
  }

  @Override
  public StreamGraph withContextManager(ContextManager contextManager) {
    this.contextManager = contextManager;
    return this;
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
      Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor, BiFunction<? super K, ? super V, ? extends M> msgBuilder) {
    String streamId = String.format("%s-%s-%s",
        config.get(JobConfig.JOB_NAME()),
        config.get(JobConfig.JOB_ID(), "1"),
        streamName);
    if (msgBuilder == null) {
      throw new IllegalArgumentException("msgBuilder cannot be null for an intermediate stream");
    }

    if (keyExtractor == null) {
      throw new IllegalArgumentException("keyExtractor can't be null for an output stream.");
    }
    if (msgExtractor == null) {
      throw new IllegalArgumentException("msgExtractor can't be null for an output stream.");
    }

    StreamSpec streamSpec = runner.getStreamSpec(streamId);
    IntermediateStreamInternalImpl<K, V, M> intStream =
        (IntermediateStreamInternalImpl<K, V, M>) inStreams
            .computeIfAbsent(streamSpec,
                k -> new IntermediateStreamInternalImpl<>(this, streamSpec, (Function<M, K>) keyExtractor,
                    (Function<M, V>) msgExtractor, (BiFunction<K, V, M>) msgBuilder));
    outStreams.putIfAbsent(streamSpec, intStream);
    return intStream;
  }

  public Map<StreamSpec, InputStreamInternal> getInputStreams() {
    return Collections.unmodifiableMap(inStreams);
  }

  public Map<StreamSpec, OutputStreamInternal> getOutputStreams() {
    return Collections.unmodifiableMap(outStreams);
  }

  public ContextManager getContextManager() {
    return this.contextManager;
  }

  /* package private */ int getNextOpId() {
    return this.opId++;
  }
}
