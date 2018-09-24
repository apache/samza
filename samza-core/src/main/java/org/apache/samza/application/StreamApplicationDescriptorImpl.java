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
package org.apache.samza.application;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.operators.functions.StreamExpander;
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
import org.apache.samza.table.hybrid.BaseHybridTableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class defines:
 * 1) an implementation of {@link StreamApplicationDescriptor} that provides APIs to access {@link MessageStream}, {@link OutputStream},
 * and {@link Table} to create the DAG of transforms.
 * 2) a builder that creates a serializable {@link OperatorSpecGraph} from user-defined DAG
 */
public class StreamApplicationDescriptorImpl extends ApplicationDescriptorImpl<StreamApplicationDescriptor>
    implements StreamApplicationDescriptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamApplicationDescriptorImpl.class);
  private static final Pattern ID_PATTERN = Pattern.compile("[\\d\\w-_]+");

  private final Map<String, InputDescriptor> inputDescriptors = new LinkedHashMap<>();
  private final Map<String, OutputDescriptor> outputDescriptors = new LinkedHashMap<>();
  private final Set<String> broadcastStreams = new HashSet<>();
  private final Map<String, TableDescriptor> tableDescriptors = new LinkedHashMap<>();
  private final Map<String, SystemDescriptor> systemDescriptors = new LinkedHashMap<>();
  // We use a LHM for deterministic order in initializing and closing operators.
  private final Map<String, InputOperatorSpec> inputOperators = new LinkedHashMap<>();
  private final Map<String, OutputStreamImpl> outputStreams = new LinkedHashMap<>();
  private final Map<TableSpec, TableImpl> tables = new LinkedHashMap<>();
  private final Set<String> operatorIds = new HashSet<>();

  private Optional<SystemDescriptor> defaultSystemDescriptorOptional = Optional.empty();
  private Optional<Serde> defaultSystemSerdeOptional = Optional.empty();

  /**
   * The 0-based position of the next operator in the graph.
   * Part of the unique ID for each OperatorSpec in the graph.
   * Should only accessed and incremented via {@link #getNextOpId(OpCode, String)}.
   */
  private int nextOpNum = 0;

  public StreamApplicationDescriptorImpl(StreamApplication userApp, Config config) {
    super(userApp, config);
    userApp.describe(this);
  }

  @Override
  public StreamApplicationDescriptor withDefaultSystem(SystemDescriptor<?> defaultSystemDescriptor, Serde defaultSystemSerde) {
    Preconditions.checkNotNull(defaultSystemDescriptor, "Provided defaultSystemDescriptor must not be null.");
    Preconditions.checkState(inputOperators.isEmpty() && outputStreams.isEmpty(),
        "Default system must be set before creating any input or output streams.");
    addSystemDescriptor(defaultSystemDescriptor);

    defaultSystemDescriptorOptional = Optional.of(defaultSystemDescriptor);
    defaultSystemSerdeOptional = Optional.ofNullable(defaultSystemSerde);
    return this;
  }

  @Override
  public <M> MessageStream<M> getInputStream(InputDescriptor<M, ?> inputDescriptor) {
    SystemDescriptor systemDescriptor = inputDescriptor.getSystemDescriptor();
    Optional<StreamExpander> expander = systemDescriptor.getExpander();
    if (expander.isPresent()) {
      return expander.get().apply(this, inputDescriptor);
    }

    // TODO: SAMZA-1841: need to add to the broadcast streams if inputDescriptor is for a broadcast stream
    Preconditions.checkState(!inputDescriptors.containsKey(inputDescriptor.getStreamId()),
        String.format("add input descriptors multiple times with the same streamId: %s", inputDescriptor.getStreamId()));
    inputDescriptors.put(inputDescriptor.getStreamId(), inputDescriptor);
    addSystemDescriptor(inputDescriptor.getSystemDescriptor());

    String streamId = inputDescriptor.getStreamId();
    Preconditions.checkState(!inputOperators.containsKey(streamId),
        "getInputStream must not be called multiple times with the same streamId: " + streamId);

    Serde serde = inputDescriptor.getSerde();
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
    InputTransformer transformer = inputDescriptor.getTransformer().orElse(null);
    InputOperatorSpec inputOperatorSpec =
        OperatorSpecs.createInputOperatorSpec(streamId, kvSerdes.getKey(), kvSerdes.getValue(),
            transformer, isKeyed, this.getNextOpId(OpCode.INPUT, null));
    inputOperators.put(streamId, inputOperatorSpec);
    return new MessageStreamImpl(this, inputOperators.get(streamId));
  }

  @Override
  public <M> OutputStream<M> getOutputStream(OutputDescriptor<M, ?> outputDescriptor) {
    Preconditions.checkState(!outputDescriptors.containsKey(outputDescriptor.getStreamId()),
        String.format("add output descriptors multiple times with the same streamId: %s", outputDescriptor.getStreamId()));
    outputDescriptors.put(outputDescriptor.getStreamId(), outputDescriptor);
    addSystemDescriptor(outputDescriptor.getSystemDescriptor());

    String streamId = outputDescriptor.getStreamId();
    Preconditions.checkState(!outputStreams.containsKey(streamId),
        "getOutputStream must not be called multiple times with the same streamId: " + streamId);

    Serde serde = outputDescriptor.getSerde();
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
  public <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDescriptor) {

    if (tableDescriptor instanceof BaseHybridTableDescriptor) {
      List<? extends TableDescriptor<K, V, ?>> tableDescs = ((BaseHybridTableDescriptor) tableDescriptor).getTableDescriptors();
      tableDescs.forEach(td -> getTable(td));
    }

    String tableId = tableDescriptor.getTableId();
    Preconditions.checkState(StringUtils.isNotBlank(tableId) && ID_PATTERN.matcher(tableId).matches(),
        String.format("tableId: %s must confirm to pattern: %s", tableId, ID_PATTERN.toString()));
    Preconditions.checkState(!tableDescriptors.containsKey(tableDescriptor.getTableId()),
        String.format("add table descriptors multiple times with the same tableId: %s", tableDescriptor.getTableId()));
    tableDescriptors.put(tableDescriptor.getTableId(), tableDescriptor);

    TableSpec tableSpec = ((BaseTableDescriptor) tableDescriptor).getTableSpec();
    if (tables.containsKey(tableSpec)) {
      throw new IllegalStateException(
          String.format("getTable() invoked multiple times with the same tableId: %s", tableId));
    }
    tables.put(tableSpec, new TableImpl(tableSpec));
    return tables.get(tableSpec);
  }

  /**
   * Get all the {@link InputDescriptor}s to this application
   *
   * @return an immutable map of streamId to {@link InputDescriptor}
   */
  @Override
  public Map<String, InputDescriptor> getInputDescriptors() {
    return Collections.unmodifiableMap(inputDescriptors);
  }

  /**
   * Get all the {@link OutputDescriptor}s from this application
   *
   * @return an immutable map of streamId to {@link OutputDescriptor}
   */
  @Override
  public Map<String, OutputDescriptor> getOutputDescriptors() {
    return Collections.unmodifiableMap(outputDescriptors);
  }

  /**
   * Get all the broadcast streamIds from this application
   *
   * @return an immutable set of streamIds
   */
  @Override
  public Set<String> getBroadcastStreams() {
    return Collections.unmodifiableSet(broadcastStreams);
  }

  /**
   * Get all the {@link TableDescriptor}s in this application
   *
   * @return an immutable set of {@link TableDescriptor}s
   */
  @Override
  public Set<TableDescriptor> getTableDescriptors() {
    return Collections.unmodifiableSet(new HashSet<>(tableDescriptors.values()));
  }

  /**
   * Get all the unique {@link SystemDescriptor}s in this application
   *
   * @return an immutable set of {@link SystemDescriptor}s
   */
  @Override
  public Set<SystemDescriptor> getSystemDescriptors() {
    // We enforce that users must not use different system descriptor instances for the same system name
    // when getting an input/output stream or setting the default system descriptor
    return Collections.unmodifiableSet(new HashSet<>(systemDescriptors.values()));
  }

  /**
   * Get the default {@link SystemDescriptor} in this application
   *
   * @return the default {@link SystemDescriptor}
   */
  @Override
  public Optional<SystemDescriptor> getDefaultSystemDescriptor() {
    return defaultSystemDescriptorOptional;
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
    if (StringUtils.isNotBlank(userDefinedId) && !ID_PATTERN.matcher(userDefinedId).matches()) {
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

  public Map<String, InputOperatorSpec> getInputOperators() {
    return Collections.unmodifiableMap(inputOperators);
  }

  public Map<String, OutputStreamImpl> getOutputStreams() {
    return Collections.unmodifiableMap(outputStreams);
  }

  public Map<TableSpec, TableImpl> getTables() {
    return Collections.unmodifiableMap(tables);
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
  public <M> IntermediateMessageStreamImpl<M> getIntermediateStream(String streamId, Serde<M> serde, boolean isBroadcast) {
    Preconditions.checkState(!inputOperators.containsKey(streamId) && !outputStreams.containsKey(streamId),
        "getIntermediateStream must not be called multiple times with the same streamId: " + streamId);

    if (serde == null) {
      LOGGER.info("No serde provided for intermediate stream: " + streamId +
          ". Key and message serdes for the default system will be used.");
    }

    if (isBroadcast) {
      broadcastStreams.add(streamId);
    }

    boolean isKeyed;
    KV<Serde, Serde> kvSerdes;
    if (serde == null) { // if no explicit serde available
      isKeyed = true; // assume keyed stream
      Serde defaultSystemSerde = defaultSystemSerdeOptional // and that either the default system serde is set,
          .orElseGet(() -> KVSerde.of(null, null)); // or that job.default.system serdes are provided in configs
      kvSerdes = getKVSerdes(streamId, defaultSystemSerde);
    } else {
      isKeyed = serde instanceof KVSerde;
      kvSerdes = getKVSerdes(streamId, serde);
    }

    InputTransformer transformer = (InputTransformer) defaultSystemDescriptorOptional
        .flatMap(SystemDescriptor::getTransformer).orElse(null);

    InputOperatorSpec inputOperatorSpec =
        OperatorSpecs.createInputOperatorSpec(streamId, kvSerdes.getKey(), kvSerdes.getValue(),
            transformer, isKeyed, this.getNextOpId(OpCode.INPUT, null));
    inputOperators.put(streamId, inputOperatorSpec);
    outputStreams.put(streamId, new OutputStreamImpl(streamId, kvSerdes.getKey(), kvSerdes.getValue(), isKeyed));
    return new IntermediateMessageStreamImpl<>(this, inputOperators.get(streamId), outputStreams.get(streamId));
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

  // check uniqueness of the {@code systemDescriptor} and add if it is unique
  private void addSystemDescriptor(SystemDescriptor systemDescriptor) {
    Preconditions.checkState(!systemDescriptors.containsKey(systemDescriptor.getSystemName())
            || systemDescriptors.get(systemDescriptor.getSystemName()) == systemDescriptor,
        "Must not use different system descriptor instances for the same system name: " + systemDescriptor.getSystemName());
    systemDescriptors.put(systemDescriptor.getSystemName(), systemDescriptor);
  }
}
