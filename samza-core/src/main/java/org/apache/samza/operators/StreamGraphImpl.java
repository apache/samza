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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A {@link StreamGraph} that provides APIs for accessing {@link MessageStream}s to be used to
 * create the DAG of transforms.
 */
public class StreamGraphImpl implements StreamGraph {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamGraphImpl.class);
  private static final Pattern USER_DEFINED_ID_PATTERN = Pattern.compile("[\\d\\w-_.]+");

  // We use a LHM for deterministic order in initializing and closing operators.
  private final Map<StreamSpec, InputOperatorSpec> inputOperators = new LinkedHashMap<>();
  private final Map<StreamSpec, OutputStreamImpl> outputStreams = new LinkedHashMap<>();
  private final Map<TableSpec, TableImpl> tables = new LinkedHashMap<>();
  private final ApplicationRunner runner;
  private final Config config;

  /**
   * The 0-based position of the next operator in the graph.
   * Part of the unique ID for each OperatorSpec in the graph.
   * Should only accessed and incremented via {@link #getNextOpId(OpCode, String)}.
   */
  private int nextOpNum = 0;
  private final Set<String> operatorIds = new HashSet<>();
  private Serde<?> defaultSerde = new KVSerde(new NoOpSerde(), new NoOpSerde());
  private ContextManager contextManager = null;

  public StreamGraphImpl(ApplicationRunner runner, Config config) {
    // TODO: SAMZA-1118 - Move StreamSpec and ApplicationRunner out of StreamGraphImpl once Systems
    // can use streamId to send and receive messages.
    this.runner = runner;
    this.config = config;
  }

  @Override
  public void setDefaultSerde(Serde<?> serde) {
    Preconditions.checkNotNull(serde, "Default serde must not be null");
    Preconditions.checkState(inputOperators.isEmpty() && outputStreams.isEmpty(),
        "Default serde must be set before creating any input or output streams.");
    this.defaultSerde = serde;
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId, Serde<M> serde) {
    StreamSpec streamSpec = runner.getStreamSpec(streamId);
    Preconditions.checkState(streamSpec != null, "No StreamSpec found for streamId: " + streamId);
    Preconditions.checkNotNull(serde, "serde must not be null for an input stream.");
    Preconditions.checkState(!inputOperators.containsKey(streamSpec),
        "getInputStream must not be called multiple times with the same streamId: " + streamId);

    KV<Serde, Serde> kvSerdes = getKVSerdes(streamId, serde);
    if (outputStreams.containsKey(streamSpec)) {
      OutputStreamImpl outputStream = outputStreams.get(streamSpec);
      Serde keySerde = outputStream.getKeySerde();
      Serde valueSerde = outputStream.getValueSerde();
      Preconditions.checkState(kvSerdes.getKey().equals(keySerde) && kvSerdes.getValue().equals(valueSerde),
          String.format("Stream %s is being used both as an input and an output stream. Serde in Samza happens at "
              + "stream level, so the same key and message Serde must be used for both.", streamId));
    }

    boolean isKeyed = serde instanceof KVSerde;
    InputOperatorSpec inputOperatorSpec =
        OperatorSpecs.createInputOperatorSpec(streamSpec, kvSerdes.getKey(), kvSerdes.getValue(),
            isKeyed, this.getNextOpId(OpCode.INPUT, null));
    inputOperators.put(streamSpec, inputOperatorSpec);
    return new MessageStreamImpl<M>(this, inputOperators.get(streamSpec));
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId) {
    return (MessageStream<M>) getInputStream(streamId, defaultSerde);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId, Serde<M> serde) {
    StreamSpec streamSpec = runner.getStreamSpec(streamId);
    Preconditions.checkState(streamSpec != null, "No StreamSpec found for streamId: " + streamId);
    Preconditions.checkNotNull(serde, "serde must not be null for an output stream.");
    Preconditions.checkState(!outputStreams.containsKey(streamSpec),
        "getOutputStream must not be called multiple times with the same streamId: " + streamId);

    KV<Serde, Serde> kvSerdes = getKVSerdes(streamId, serde);
    if (inputOperators.containsKey(streamSpec)) {
      InputOperatorSpec inputOperatorSpec = inputOperators.get(streamSpec);
      Serde keySerde = inputOperatorSpec.getKeySerde();
      Serde valueSerde = inputOperatorSpec.getValueSerde();
      Preconditions.checkState(kvSerdes.getKey().equals(keySerde) && kvSerdes.getValue().equals(valueSerde),
          String.format("Stream %s is being used both as an input and an output stream. Serde in Samza happens at "
              + "stream level, so the same key and message Serde must be used for both.", streamId));
    }

    boolean isKeyed = serde instanceof KVSerde;
    outputStreams.put(streamSpec, new OutputStreamImpl<>(streamSpec, kvSerdes.getKey(), kvSerdes.getValue(), isKeyed));
    return outputStreams.get(streamSpec);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId) {
    return (OutputStream<M>) getOutputStream(streamId, defaultSerde);
  }

  @Override
  public <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDesc) {
    TableSpec tableSpec = ((BaseTableDescriptor) tableDesc).getTableSpec();
    if (tables.containsKey(tableSpec)) {
      throw new IllegalStateException(String.format(
          "getTable() invoked multiple times with the same tableId: %s",
          tableDesc.getTableId()));
    }
    tables.put(tableSpec, new TableImpl(tableSpec));
    return tables.get(tableSpec);
  }

  @Override
  public StreamGraph withContextManager(ContextManager contextManager) {
    this.contextManager = contextManager;
    return this;
  }

  /**
   * See {@link StreamGraphImpl#getIntermediateStream(String, Serde, boolean)}.
   */
  @VisibleForTesting
  public <M> IntermediateMessageStreamImpl<M> getIntermediateStream(String streamId, Serde<M> serde) {
    return getIntermediateStream(streamId, serde, false);
  }

  /**
   * Internal helper for {@link MessageStreamImpl} to add an intermediate {@link MessageStream} to the graph.
   * An intermediate {@link MessageStream} is both an output and an input stream.
   *
   * @param streamId the id of the stream to be created.
   * @param serde the {@link Serde} to use for the message in the intermediate stream. If null, the default serde
   *              is used.
   * @param isBroadcast whether the stream is a broadcast stream.
   * @param <M> the type of messages in the intermediate {@link MessageStream}
   * @return  the intermediate {@link MessageStreamImpl}
   *
   * TODO: once SAMZA-1566 is resolved, we should be able to pass in the StreamSpec directly.
   */
  @VisibleForTesting
  <M> IntermediateMessageStreamImpl<M> getIntermediateStream(String streamId, Serde<M> serde, boolean isBroadcast) {
    StreamSpec streamSpec = runner.getStreamSpec(streamId);
    if (isBroadcast) {
      streamSpec = streamSpec.copyWithBroadCast();
    }

    Preconditions.checkState(!inputOperators.containsKey(streamSpec) && !outputStreams.containsKey(streamSpec),
        "getIntermediateStream must not be called multiple times with the same streamId: " + streamId);

    if (serde == null) {
      LOGGER.info("Using default serde for intermediate stream: " + streamId);
      serde = (Serde<M>) defaultSerde;
    }

    boolean isKeyed = serde instanceof KVSerde;
    KV<Serde, Serde> kvSerdes = getKVSerdes(streamId, serde);
    InputOperatorSpec inputOperatorSpec =
        OperatorSpecs.createInputOperatorSpec(streamSpec, kvSerdes.getKey(), kvSerdes.getValue(),
            isKeyed, this.getNextOpId(OpCode.INPUT, null));
    inputOperators.put(streamSpec, inputOperatorSpec);
    outputStreams.put(streamSpec, new OutputStreamImpl(streamSpec, kvSerdes.getKey(), kvSerdes.getValue(), isKeyed));
    return new IntermediateMessageStreamImpl<M>(this, inputOperators.get(streamSpec), outputStreams.get(streamSpec));
  }

  public Map<StreamSpec, InputOperatorSpec> getInputOperators() {
    return Collections.unmodifiableMap(inputOperators);
  }

  public Map<StreamSpec, OutputStreamImpl> getOutputStreams() {
    return Collections.unmodifiableMap(outputStreams);
  }

  public Map<TableSpec, TableImpl> getTables() {
    return Collections.unmodifiableMap(tables);
  }

  public ContextManager getContextManager() {
    return this.contextManager;
  }

  /**
   * Gets the unique ID for the next operator in the graph. The ID is of the following format:
   * jobName-jobId-opCode-(userDefinedId|nextOpNum);
   *
   * @param opCode the {@link OpCode} of the next operator
   * @param userDefinedId the optional user-provided name of the next operator or null
   * @return the unique ID for the next operator in the graph
   */
  /* package private */ String getNextOpId(OpCode opCode, String userDefinedId) {
    if (StringUtils.isNotBlank(userDefinedId) && !USER_DEFINED_ID_PATTERN.matcher(userDefinedId).matches()) {
      throw new SamzaException("Operator ID must not contain spaces and special characters: " + userDefinedId);
    }

    String nextOpId = String.format("%s-%s-%s-%s",
        config.get(JobConfig.JOB_NAME()),
        config.get(JobConfig.JOB_ID(), "1"),
        opCode.name().toLowerCase(),
        StringUtils.isNotBlank(userDefinedId) ? userDefinedId.trim() : String.valueOf(nextOpNum));
    if (!operatorIds.add(nextOpId)) {
      throw new SamzaException(
          String.format("Found duplicate operator ID %s in the graph. Operator IDs must be unique.", nextOpId));
    }
    nextOpNum++;
    return nextOpId;
  }

  /**
   * Gets the unique ID for the next operator in the graph. The ID is of the following format:
   * jobName-jobId-opCode-nextOpNum;
   *
   * @param opCode the {@link OpCode} of the next operator
   * @return the unique ID for the next operator in the graph
   */
  /* package private */ String getNextOpId(OpCode opCode) {
    return getNextOpId(opCode, null);
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

  private KV<Serde, Serde> getKVSerdes(String streamId, Serde serde) {
    Serde keySerde, valueSerde;

    if (serde instanceof KVSerde) {
      keySerde = ((KVSerde) serde).getKeySerde();
      valueSerde = ((KVSerde) serde).getValueSerde();
    } else {
      keySerde = new NoOpSerde();
      valueSerde = serde;
    }

    if (keySerde instanceof NoOpSerde) {
      LOGGER.info("Using NoOpSerde as the key serde for stream " + streamId +
          ". Keys will not be (de)serialized");
    }
    if (valueSerde instanceof NoOpSerde) {
      LOGGER.info("Using NoOpSerde as the value serde for stream " + streamId +
          ". Values will not be (de)serialized");
    }

    return KV.of(keySerde, valueSerde);
  }
}
