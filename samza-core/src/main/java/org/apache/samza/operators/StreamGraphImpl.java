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

import com.google.common.base.Preconditions;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link StreamGraph} that provides APIs for accessing {@link MessageStream}s to be used to
 * create the DAG of transforms.
 */
public class StreamGraphImpl implements StreamGraph {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamGraphImpl.class);

  /**
   * Unique identifier for each {@link org.apache.samza.operators.spec.OperatorSpec} in the graph.
   * Should only be accessed by {@link MessageStreamImpl} via {@link #getNextOpId()}.
   */
  private int opId = 0;

  // We use a LHM for deterministic order in initializing and closing operators.
  private final Map<StreamSpec, InputOperatorSpec> inputOperators = new LinkedHashMap<>();
  private final Map<StreamSpec, OutputStreamImpl> outputStreams = new LinkedHashMap<>();
  private final ApplicationRunner runner;
  private final Config config;

  private Serde<?> defaultKeySerde = new NoOpSerde();
  private Serde<?> defaultMsgSerde = new NoOpSerde();
  private ContextManager contextManager = null;

  public StreamGraphImpl(ApplicationRunner runner, Config config) {
    // TODO: SAMZA-1118 - Move StreamSpec and ApplicationRunner out of StreamGraphImpl once Systems
    // can use streamId to send and receive messages.
    this.runner = runner;
    this.config = config;
  }

  @Override
  public void setDefaultKeySerde(Serde<?> keySerde) {
    Preconditions.checkNotNull(keySerde, "defaultKeySerde must not be null");
    this.defaultKeySerde = keySerde;
  }

  @Override
  public void setDefaultMsgSerde(Serde<?> msgSerde) {
    Preconditions.checkNotNull(msgSerde, "defaultMsgSerde must not be null");
    this.defaultMsgSerde = msgSerde;
  }

  @Override
  public <K, V, M> MessageStream<M> getInputStream(String streamId,
      Serde<K> keySerde, Serde<V> valueSerde,
      BiFunction<? super K, ? super V, ? extends M> msgBuilder) {
    Preconditions.checkNotNull(keySerde, "keySerde must not be null for an input stream.");
    Preconditions.checkNotNull(valueSerde, "valueSerde must not be null for an input stream.");
    Preconditions.checkNotNull(msgBuilder, "msgBuilder must not be null for an input stream.");
    Preconditions.checkState(!inputOperators.containsKey(runner.getStreamSpec(streamId)),
        "getInputStream must not be called multiple times with the same streamId: " + streamId);

    StreamSpec streamSpec = runner.getStreamSpec(streamId);
    inputOperators.put(streamSpec,
        new InputOperatorSpec<>(streamSpec, keySerde, valueSerde,
            (BiFunction<K, V, M>) msgBuilder, this.getNextOpId()));
    return new MessageStreamImpl<>(this, inputOperators.get(streamSpec));
  }

  @Override
  public <K, V, M> MessageStream<M> getInputStream(String streamId,
      BiFunction<? super K, ? super V, ? extends M> msgBuilder) {
    if (defaultKeySerde instanceof NoOpSerde) {
      LOGGER.info("Using NoOpSerde as the default key serde for input stream " + streamId +
          ". Keys will not be deserialized");
    }
    if (defaultMsgSerde instanceof NoOpSerde) {
      LOGGER.info("Using NoOpSerde as the default msg serde for input stream " + streamId +
          ". Values will not be deserialized");
    }
    return getInputStream(streamId, (Serde<K>) defaultKeySerde, (Serde<V>) defaultMsgSerde, msgBuilder);
  }

  @Override
  public <K, V, M> OutputStream<K, V, M> getOutputStream(String streamId,
      Serde<K> keySerde, Serde<V> msgSerde,
      Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor) {
    Preconditions.checkNotNull(keySerde, "keySerde must not be null for an output stream.");
    Preconditions.checkNotNull(msgSerde, "msgSerde must not be null for an output stream.");
    Preconditions.checkNotNull(keyExtractor, "keyExtractor must not be null for an output stream.");
    Preconditions.checkNotNull(msgExtractor, "msgExtractor must not be null for an output stream.");
    Preconditions.checkState(!outputStreams.containsKey(runner.getStreamSpec(streamId)),
        "getOutputStream must not be called multiple times with the same streamId: " + streamId);

    StreamSpec streamSpec = runner.getStreamSpec(streamId);
    outputStreams.put(streamSpec,
        new OutputStreamImpl<>(streamSpec, keySerde, msgSerde,
            (Function<M, K>) keyExtractor, (Function<M, V>) msgExtractor));
    return outputStreams.get(streamSpec);
  }

  @Override
  public <K, V, M> OutputStream<K, V, M> getOutputStream(String streamId,
      Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor) {
    if (defaultKeySerde instanceof NoOpSerde) {
      LOGGER.info("Using NoOpSerde as the default key serde for output stream " + streamId +
          ". Keys will not be serialized");
    }
    if (defaultMsgSerde instanceof NoOpSerde) {
      LOGGER.info("Using NoOpSerde as the default msg serde for output stream " + streamId +
          ". Messages will not be serialized");
    }
    return getOutputStream(streamId, (Serde<K>) defaultKeySerde, (Serde<V>) defaultMsgSerde,
        keyExtractor, msgExtractor);
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
   * @param keySerde the {@link Serde} to use for the key in the intermediate message
   * @param msgSerde the {@link Serde} to use for the message in the intermediate message
   * @param keyExtractor the {@link Function} to extract the outgoing key from the intermediate message
   * @param msgExtractor the {@link Function} to extract the outgoing message from the intermediate message
   * @param msgBuilder the {@link BiFunction} to convert the incoming key and message to a message
   *                   in the intermediate {@link MessageStream}
   * @param <K> the type of key in the intermediate message
   * @param <V> the type of message in the intermediate message
   * @param <M> the type of messages in the intermediate {@link MessageStream}
   * @return  the intermediate {@link MessageStreamImpl}
   */
  <K, V, M> IntermediateMessageStreamImpl<K, V, M> getIntermediateStream(String streamName,
      Serde<K> keySerde, Serde<V> msgSerde,
      Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor,
      BiFunction<? super K, ? super V, ? extends M> msgBuilder) {
    String streamId = String.format("%s-%s-%s",
        config.get(JobConfig.JOB_NAME()),
        config.get(JobConfig.JOB_ID(), "1"),
        streamName);
    StreamSpec streamSpec = runner.getStreamSpec(streamId);

    Preconditions.checkNotNull(keySerde, "keySerde must not be null for an intermediate stream.");
    Preconditions.checkNotNull(msgSerde, "msgSerde must not be null for an intermediate stream.");
    Preconditions.checkNotNull(keyExtractor, "keyExtractor must not be null for an intermediate stream.");
    Preconditions.checkNotNull(msgExtractor, "msgExtractor must not be null for an intermediate stream.");
    Preconditions.checkNotNull(msgBuilder, "msgBuilder must not be null for an intermediate stream.");
    Preconditions.checkState(!inputOperators.containsKey(streamSpec) && !outputStreams.containsKey(streamSpec),
        "getIntermediateStream must not be called multiple times with the same streamId: " + streamId);

    inputOperators.put(streamSpec,
        new InputOperatorSpec(streamSpec, keySerde, msgSerde, msgBuilder, this.getNextOpId()));
    outputStreams.put(streamSpec,
        new OutputStreamImpl(streamSpec, keySerde, msgSerde, keyExtractor, msgExtractor));
    return new IntermediateMessageStreamImpl<>(this, inputOperators.get(streamSpec), outputStreams.get(streamSpec));
  }

  <K, V, M> IntermediateMessageStreamImpl<K, V, M> getIntermediateStream(String streamName,
      Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor,
      BiFunction<? super K, ? super V, ? extends M> msgBuilder) {
    if (defaultKeySerde instanceof NoOpSerde) {
      LOGGER.info("Using NoOpSerde as the default key serde for intermediate stream " + streamName +
          ". Keys will not be (de)serialized");
    }
    if (defaultMsgSerde instanceof NoOpSerde) {
      LOGGER.info("Using NoOpSerde as the default msg serde for intermediate stream " + streamName +
          ". Keys will not be (de)serialized");
    }
    return getIntermediateStream(streamName, (Serde<K>) defaultKeySerde, (Serde<V>) defaultMsgSerde,
        keyExtractor, msgExtractor, msgBuilder);
  }

  public Map<StreamSpec, InputOperatorSpec> getInputOperators() {
    return Collections.unmodifiableMap(inputOperators);
  }

  public Map<StreamSpec, OutputStreamImpl> getOutputStreams() {
    return Collections.unmodifiableMap(outputStreams);
  }

  public ContextManager getContextManager() {
    return this.contextManager;
  }

  /* package private */ int getNextOpId() {
    return this.opId++;
  }

  /**
   * Get all {@link OperatorSpec}s available in this {@link StreamGraphImpl}
   *
   * @return  all available {@link OperatorSpec}s
   */
  public Collection<OperatorSpec> getAllOperatorSpecs() {
    Collection<InputOperatorSpec> inputOperatorSpecs = inputOperators.values();
    Set<OperatorSpec> operatorSpecs = new HashSet<>();
    for (InputOperatorSpec inputOperatorSpec: inputOperatorSpecs) {
      operatorSpecs.add(inputOperatorSpec);
      doGetOperatorSpecs(inputOperatorSpec, operatorSpecs);
    }
    return operatorSpecs;
  }

  private void doGetOperatorSpecs(OperatorSpec operatorSpec, Set<OperatorSpec> specs) {
    Collection<OperatorSpec> registeredOperatorSpecs = operatorSpec.getRegisteredOperatorSpecs();
    for (OperatorSpec registeredOperatorSpec: registeredOperatorSpecs) {
      specs.add(registeredOperatorSpec);
      doGetOperatorSpecs(registeredOperatorSpec, specs);
    }
  }

  /**
   * Returns <tt>true</tt> iff this {@link StreamGraphImpl} contains a join or a window operator
   *
   * @return  <tt>true</tt> iff this {@link StreamGraphImpl} contains a join or a window operator
   */
  public boolean hasWindowOrJoins() {
    // Obtain the operator specs from the streamGraph
    Collection<OperatorSpec> operatorSpecs = getAllOperatorSpecs();

    Set<OperatorSpec> windowOrJoinSpecs = operatorSpecs.stream()
        .filter(spec -> spec.getOpCode() == OperatorSpec.OpCode.WINDOW || spec.getOpCode() == OperatorSpec.OpCode.JOIN)
        .collect(Collectors.toSet());

    return windowOrJoinSpecs.size() != 0;
  }
}
