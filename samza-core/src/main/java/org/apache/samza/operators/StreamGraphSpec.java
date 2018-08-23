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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * This class defines:
 * 1) an implementation of {@link StreamGraph} that provides APIs for accessing {@link MessageStream}s to be used to
 * create the DAG of transforms.
 * 2) a builder that creates a serializable {@link OperatorSpecGraph} from user-defined DAG
 */
public class StreamGraphSpec implements StreamGraph {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamGraphSpec.class);
  public static final Pattern STREAM_ID_PATTERN = Pattern.compile("[\\d\\w-_.]+");
  public static final Pattern TABLE_ID_PATTERN = Pattern.compile("[\\d\\w-_]+");

  // We use a LHM for deterministic order in initializing and closing operators.
  private final Map<String, InputOperatorSpec> inputOperators = new LinkedHashMap<>();
  private final Map<String, OutputStreamImpl> outputStreams = new LinkedHashMap<>();
  private final Set<String> broadcastStreams = new HashSet<>();
  private final Map<TableSpec, TableImpl> tables = new LinkedHashMap<>();
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

  public StreamGraphSpec(Config config) {
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
    Preconditions.checkState(isValidStreamId(streamId), String.format(
        "streamId %s doesn't confirm to pattern %s", streamId, StreamGraphSpec.STREAM_ID_PATTERN));
    Preconditions.checkNotNull(serde, "serde must not be null for an input stream.");
    Preconditions.checkState(!inputOperators.containsKey(streamId),
        "getInputStream must not be called multiple times with the same streamId: " + streamId);

    KV<Serde, Serde> kvSerdes = getKVSerdes(streamId, serde);
    if (outputStreams.containsKey(streamId)) {
      OutputStreamImpl outputStream = outputStreams.get(streamId);
      Serde keySerde = outputStream.getKeySerde();
      Serde valueSerde = outputStream.getValueSerde();
      Preconditions.checkState(kvSerdes.getKey().equals(keySerde) && kvSerdes.getValue().equals(valueSerde),
          String.format("Stream %s is being used both as an input and an output stream. Serde in Samza happens at "
              + "stream level, so the same key and message Serde must be used for both.", streamId));
    }

    boolean isKeyed = serde instanceof KVSerde;
    InputOperatorSpec inputOperatorSpec =
        OperatorSpecs.createInputOperatorSpec(streamId, kvSerdes.getKey(), kvSerdes.getValue(),
            isKeyed, this.getNextOpId(OpCode.INPUT, null));
    inputOperators.put(streamId, inputOperatorSpec);
    return new MessageStreamImpl<>(this, inputOperators.get(streamId));
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId) {
    return (MessageStream<M>) getInputStream(streamId, defaultSerde);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId, Serde<M> serde) {
    Preconditions.checkState(isValidStreamId(streamId), String.format(
        "streamId %s doesn't confirm to pattern %s", streamId, StreamGraphSpec.STREAM_ID_PATTERN));
    Preconditions.checkNotNull(serde, "serde must not be null for an output stream.");
    Preconditions.checkState(!outputStreams.containsKey(streamId),
        "getOutputStream must not be called multiple times with the same streamId: " + streamId);

    KV<Serde, Serde> kvSerdes = getKVSerdes(streamId, serde);
    if (inputOperators.containsKey(streamId)) {
      InputOperatorSpec inputOperatorSpec = inputOperators.get(streamId);
      Serde keySerde = inputOperatorSpec.getKeySerde();
      Serde valueSerde = inputOperatorSpec.getValueSerde();
      Preconditions.checkState(kvSerdes.getKey().equals(keySerde) && kvSerdes.getValue().equals(valueSerde),
          String.format("Stream %s is being used both as an input and an output stream. Serde in Samza happens at "
              + "stream level, so the same key and message Serde must be used for both.", streamId));
    }

    boolean isKeyed = serde instanceof KVSerde;
    outputStreams.put(streamId, new OutputStreamImpl<>(streamId, kvSerdes.getKey(), kvSerdes.getValue(), isKeyed));
    return outputStreams.get(streamId);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId) {
    return (OutputStream<M>) getOutputStream(streamId, defaultSerde);
  }

  @Override
  public <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDesc) {
    Preconditions.checkState(isValidTableId(tableDesc.getTableId()), String.format(
        "tableId %s doesn't confirm to pattern %s", tableDesc.getTableId(), TABLE_ID_PATTERN.toString()));
    TableSpec tableSpec = ((BaseTableDescriptor) tableDesc).getTableSpec();
    Preconditions.checkState(!tables.containsKey(tableSpec), String.format(
        "getTable() invoked multiple times with the same tableId: %s", tableDesc.getTableId()));
    tables.put(tableSpec, new TableImpl(tableSpec));
    return tables.get(tableSpec);
  }

  @Override
  public StreamGraph withContextManager(ContextManager contextManager) {
    this.contextManager = contextManager;
    return this;
  }

  public ContextManager getContextManager() {
    return this.contextManager;
  }

  public OperatorSpecGraph getOperatorSpecGraph() {
    return new OperatorSpecGraph(this);
  }

  /**
   * Gets the unique ID for the next operator in the graph. The ID is of the following format:
   * jobName-jobId-opCode-(userDefinedId|nextOpNum);
   *
   * @param opCode the {@link OpCode} of the next operator
   * @param userDefinedId the optional user-provided name of the next operator or null
   * @return the unique ID for the next operator in the graph
   */
  public String getNextOpId(OpCode opCode, String userDefinedId) {
    if (StringUtils.isNotBlank(userDefinedId) && !STREAM_ID_PATTERN.matcher(userDefinedId).matches()) {
      throw new SamzaException("Operator ID must not contain spaces or special characters: " + userDefinedId);
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
  public String getNextOpId(OpCode opCode) {
    return getNextOpId(opCode, null);
  }

  /**
   * See {@link StreamGraphSpec#getIntermediateStream(String, Serde, boolean)}.
   *
   * @param <M> type of messages in the intermediate stream
   * @param streamId the id of the stream to be created
   * @param serde the {@link Serde} to use for messages in the intermediate stream. If null, the default serde is used.
   * @return  the intermediate {@link MessageStreamImpl}
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
   */
  @VisibleForTesting
  <M> IntermediateMessageStreamImpl<M> getIntermediateStream(String streamId, Serde<M> serde, boolean isBroadcast) {
    Preconditions.checkState(!inputOperators.containsKey(streamId) && !outputStreams.containsKey(streamId),
        "getIntermediateStream must not be called multiple times with the same streamId: " + streamId);

    if (serde == null) {
      LOGGER.info("Using default serde for intermediate stream: " + streamId);
      serde = (Serde<M>) defaultSerde;
    }

    if (isBroadcast) broadcastStreams.add(streamId);
    boolean isKeyed = serde instanceof KVSerde;
    KV<Serde, Serde> kvSerdes = getKVSerdes(streamId, serde);
    InputOperatorSpec inputOperatorSpec =
        OperatorSpecs.createInputOperatorSpec(streamId, kvSerdes.getKey(), kvSerdes.getValue(),
            isKeyed, this.getNextOpId(OpCode.INPUT, null));
    inputOperators.put(streamId, inputOperatorSpec);
    outputStreams.put(streamId, new OutputStreamImpl(streamId, kvSerdes.getKey(), kvSerdes.getValue(), isKeyed));
    return new IntermediateMessageStreamImpl<>(this, inputOperators.get(streamId), outputStreams.get(streamId));
  }

  Map<String, InputOperatorSpec> getInputOperators() {
    return Collections.unmodifiableMap(inputOperators);
  }

  Map<String, OutputStreamImpl> getOutputStreams() {
    return Collections.unmodifiableMap(outputStreams);
  }

  Set<String> getBroadcastStreams() {
    return Collections.unmodifiableSet(broadcastStreams);
  }

  Map<TableSpec, TableImpl> getTables() {
    return Collections.unmodifiableMap(tables);
  }

  public static boolean isValidStreamId(String id) {
    return StringUtils.isNotBlank(id) && STREAM_ID_PATTERN.matcher(id).matches();
  }

  public static boolean isValidTableId(String id) {
    return StringUtils.isNotBlank(id) && TABLE_ID_PATTERN.matcher(id).matches();
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
