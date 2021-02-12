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
package org.apache.samza.application.descriptors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.table.descriptors.LocalTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.system.descriptors.InputTransformer;
import org.apache.samza.system.descriptors.StreamExpander;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.Table;

/**
 * This class defines:
 * 1) an implementation of {@link StreamApplicationDescriptor} that provides APIs to access {@link MessageStream}, {@link OutputStream},
 * and {@link Table} to create the DAG of transforms.
 * 2) a builder that creates a serializable {@link OperatorSpecGraph} from user-defined DAG
 */
@SuppressWarnings("rawtypes")
public class StreamApplicationDescriptorImpl extends ApplicationDescriptorImpl<StreamApplicationDescriptor>
    implements StreamApplicationDescriptor {

  // We use a LHMs for deterministic order in initializing and closing operators.
  private final Set<String> intermediateBroadcastStreamIds = new HashSet<>();
  private final Map<String, InputOperatorSpec> inputOperators = new LinkedHashMap<>();
  private final Map<String, OutputStreamImpl> outputStreams = new LinkedHashMap<>();
  private final Set<String> operatorIds = new HashSet<>();

  /**
   * The 0-based position of the next operator in the graph.
   * Part of the unique ID for each OperatorSpec in the graph.
   * Should only accessed and incremented via {@link #getNextOpId(String, OpCode, String)}.
   */
  private int nextOpNum = 0;

  public StreamApplicationDescriptorImpl(StreamApplication userApp, Config config) {
    super(userApp, config);
    userApp.describe(this);
  }

  @Override
  public <M> MessageStream<M> getInputStream(InputDescriptor<M, ?> inputDescriptor) {
    SystemDescriptor systemDescriptor = inputDescriptor.getSystemDescriptor();
    Optional<StreamExpander> expander = systemDescriptor.getExpander();
    if (expander.isPresent()) {
      return expander.get().apply(this, inputDescriptor);
    }

    // TODO: SAMZA-1841: need to add to the broadcast streams if inputDescriptor is for a broadcast stream
    addInputDescriptor(inputDescriptor);

    String streamId = inputDescriptor.getStreamId();
    Serde serde = inputDescriptor.getSerde();
    KV<Serde, Serde> kvSerdes = getOrCreateStreamSerdes(streamId, serde);
    boolean isKeyed = serde instanceof KVSerde;
    InputTransformer transformer = inputDescriptor.getTransformer().orElse(null);
    InputOperatorSpec inputOperatorSpec =
        OperatorSpecs.createInputOperatorSpec(streamId, kvSerdes.getKey(), kvSerdes.getValue(),
            transformer, isKeyed, this.getNextOpId(null, OpCode.INPUT, null));
    inputOperators.put(streamId, inputOperatorSpec);
    return new MessageStreamImpl(this, inputOperators.get(streamId));
  }

  @Override
  public <M> OutputStream<M> getOutputStream(OutputDescriptor<M, ?> outputDescriptor) {
    addOutputDescriptor(outputDescriptor);

    String streamId = outputDescriptor.getStreamId();
    Serde serde = outputDescriptor.getSerde();
    KV<Serde, Serde> kvSerdes = getOrCreateStreamSerdes(streamId, serde);
    boolean isKeyed = serde instanceof KVSerde;
    outputStreams.put(streamId, new OutputStreamImpl<>(streamId, kvSerdes.getKey(), kvSerdes.getValue(), isKeyed));
    return outputStreams.get(streamId);
  }

  @Override
  public <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDescriptor) {
    addTableDescriptor(tableDescriptor);
    if (tableDescriptor instanceof LocalTableDescriptor) {
      LocalTableDescriptor localTableDescriptor = (LocalTableDescriptor) tableDescriptor;
      getOrCreateTableSerdes(localTableDescriptor.getTableId(), localTableDescriptor.getSerde());
    }
    return new TableImpl(tableDescriptor);
  }

  @Override
  public Set<String> getInputStreamIds() {
    return Collections.unmodifiableSet(new HashSet<>(inputOperators.keySet()));
  }

  @Override
  public Set<String> getOutputStreamIds() {
    return Collections.unmodifiableSet(new HashSet<>(outputStreams.keySet()));
  }

  @Override
  public Set<String> getIntermediateBroadcastStreamIds() {
    return Collections.unmodifiableSet(intermediateBroadcastStreamIds);
  }

  public Map<String, InputOperatorSpec> getInputOperators() {
    return Collections.unmodifiableMap(inputOperators);
  }

  public Map<String, OutputStreamImpl> getOutputStreams() {
    return Collections.unmodifiableMap(outputStreams);
  }

  public OperatorSpecGraph getOperatorSpecGraph() {
    return new OperatorSpecGraph(this);
  }

  /**
   * Gets the unique ID for the next operator in the graph. The ID is of the following format:
   * jobName-jobId-opCode-(userDefinedId|nextOpNum);
   *
   * @param desc customized description of the operator
   * @param opCode the {@link OpCode} of the next operator
   * @param userDefinedId the optional user-provided name of the next operator or null
   * @return the unique ID for the next operator in the graph
   */
  public String getNextOpId(String desc, OpCode opCode, String userDefinedId) {
    if (StringUtils.isNotBlank(desc) && !ID_PATTERN.matcher(desc).matches()) {
      throw new SamzaException("Operator description must not contain spaces or special characters: " + desc);
    }

    if (StringUtils.isNotBlank(userDefinedId) && !ID_PATTERN.matcher(userDefinedId).matches()) {
      throw new SamzaException("Operator ID must not contain spaces or special characters: " + userDefinedId);
    }

    ApplicationConfig applicationConfig = new ApplicationConfig(getConfig());
    StringBuilder sb = new StringBuilder()
        .append(applicationConfig.getAppName())
        .append("-")
        .append(applicationConfig.getAppId())
        .append("-")
        .append(opCode.name().toLowerCase())
        .append("-");

    if (StringUtils.isNotBlank(desc)) {
      sb.append(desc).append("-");
    }

    if (StringUtils.isNotBlank(userDefinedId)) {
      sb.append(userDefinedId.trim());
    } else {
      sb.append(nextOpNum);
    }

    String nextOpId = sb.toString();
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
   * @param desc customized description of the operator
   * @param opCode the {@link OpCode} of the next operator
   * @return the unique ID for the next operator in the graph
   */
  public String getNextOpId(String desc, OpCode opCode) {
    return getNextOpId(desc, opCode, null);
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
    Preconditions.checkNotNull(serde, "serde must not be null for intermediate stream: " + streamId);
    Preconditions.checkState(!inputOperators.containsKey(streamId) && !outputStreams.containsKey(streamId),
        "getIntermediateStream must not be called multiple times with the same streamId: " + streamId);

    if (isBroadcast) {
      intermediateBroadcastStreamIds.add(streamId);
    }

    boolean isKeyed = serde instanceof KVSerde;
    KV<Serde, Serde> kvSerdes = getOrCreateStreamSerdes(streamId, serde);

    InputTransformer transformer = (InputTransformer) getDefaultSystemDescriptor()
        .flatMap(SystemDescriptor::getTransformer).orElse(null);

    InputOperatorSpec inputOperatorSpec =
        OperatorSpecs.createInputOperatorSpec(streamId, kvSerdes.getKey(), kvSerdes.getValue(),
            transformer, isKeyed, this.getNextOpId(null, OpCode.INPUT, null));
    inputOperators.put(streamId, inputOperatorSpec);
    outputStreams.put(streamId, new OutputStreamImpl(streamId, kvSerdes.getKey(), kvSerdes.getValue(), isKeyed));
    return new IntermediateMessageStreamImpl<>(this, inputOperators.get(streamId), outputStreams.get(streamId));
  }
}
