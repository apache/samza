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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

/**
 * The implementation of {@link StreamGraph} interface. This class provides implementation of methods to allow users to
 * create system input/output/intermediate streams.
 */
public class StreamGraphImpl implements StreamGraph {

  /**
   * Unique identifier for each {@link org.apache.samza.operators.spec.OperatorSpec} added to transform the {@link MessageEnvelope}
   * in the input {@link MessageStream}s.
   */
  private int opId = 0;

  // TODO: SAMZA-1101: the instantiation of physical streams and the physical sink functions should be delayed
  // after physical deployment. The input/output/intermediate stream creation should also be delegated to {@link ExecutionEnvironment}
  // s.t. we can allow different physical instantiation of stream under different execution environment w/o code change.
  private class InputStreamImpl<K, V, M extends MessageEnvelope<K, V>> extends MessageStreamImpl<M> {
    final StreamSpec spec;
    final Serde<K> keySerde;
    final Serde<V> msgSerde;

    InputStreamImpl(StreamGraphImpl graph, StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
      super(graph);
      this.spec = streamSpec;
      this.keySerde = keySerde;
      this.msgSerde = msgSerde;
    }

    StreamSpec getSpec() {
      return this.spec;
    }

  }

  private class OutputStreamImpl<K, V, M extends MessageEnvelope<K, V>> implements OutputStream<M> {
    final StreamSpec spec;
    final Serde<K> keySerde;
    final Serde<V> msgSerde;

    OutputStreamImpl(StreamGraphImpl graph, StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
      this.spec = streamSpec;
      this.keySerde = keySerde;
      this.msgSerde = msgSerde;
    }

    StreamSpec getSpec() {
      return this.spec;
    }

    @Override
    public SinkFunction<M> getSinkFunction() {
      return (M message, MessageCollector mc, TaskCoordinator tc) -> {
        // TODO: need to find a way to directly pass in the serde class names
        // mc.send(new OutgoingMessageEnvelope(this.spec.getSystemStream(), this.keySerde.getClass().getName(), this.msgSerde.getClass().getName(),
        //    message.getKey(), message.getKey(), message.getMessage()));
        mc.send(new OutgoingMessageEnvelope(new SystemStream(this.spec.getSystemName(), this.spec.getPhysicalName()), message.getKey(), message.getMessage()));
      };
    }
  }

  private class IntermediateStreamImpl<PK, K, V, M extends MessageEnvelope<K, V>> extends InputStreamImpl<K, V, M> implements OutputStream<M> {
    final Function<M, PK> parKeyFn;

    /**
     * Default constructor
     *
     * @param graph the {@link StreamGraphImpl} object that this stream belongs to
     */
    IntermediateStreamImpl(StreamGraphImpl graph, StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
      this(graph, streamSpec, keySerde, msgSerde, null);
    }

    IntermediateStreamImpl(StreamGraphImpl graph, StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde, Function<M, PK> parKeyFn) {
      super(graph, streamSpec, keySerde, msgSerde);
      this.parKeyFn = parKeyFn;
    }

    @Override
    public SinkFunction<M> getSinkFunction() {
      return (M message, MessageCollector mc, TaskCoordinator tc) -> {
        // TODO: need to find a way to directly pass in the serde class names
        // mc.send(new OutgoingMessageEnvelope(this.spec.getSystemStream(), this.keySerde.getClass().getName(), this.msgSerde.getClass().getName(),
        //    message.getKey(), message.getKey(), message.getMessage()));
        if (this.parKeyFn == null) {
          mc.send(new OutgoingMessageEnvelope(new SystemStream(this.spec.getSystemName(), this.spec.getPhysicalName()), message.getKey(), message.getMessage()));
        } else {
          // apply partition key function
          mc.send(new OutgoingMessageEnvelope(new SystemStream(this.spec.getSystemName(), this.spec.getPhysicalName()), this.parKeyFn.apply(message), message.getKey(), message.getMessage()));
        }
      };
    }
  }

  /**
   * Maps keeping all {@link SystemStream}s that are input and output of operators in {@link StreamGraphImpl}
   */
  private final Map<String, MessageStream> inStreams = new HashMap<>();
  private final Map<String, OutputStream> outStreams = new HashMap<>();
  private final ExecutionEnvironment executionEnvironment;

  private ContextManager contextManager = new ContextManager() { };

  public StreamGraphImpl(ExecutionEnvironment executionEnvironment) {
    this.executionEnvironment = executionEnvironment;
  }

  @Override
  public <K, V, M extends MessageEnvelope<K, V>> MessageStream<M> createInStream(StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
    if (!this.inStreams.containsKey(streamSpec.getId())) {
      this.inStreams.putIfAbsent(streamSpec.getId(), new InputStreamImpl<K, V, M>(this, streamSpec, keySerde, msgSerde));
    }
    return this.inStreams.get(streamSpec.getId());
  }

  /**
   * Helper method to be used by {@link MessageStreamImpl} class
   *
   * @param streamSpec  the {@link StreamSpec} object defining the {@link SystemStream} as the output
   * @param <M>  the type of {@link MessageEnvelope}s in the output {@link SystemStream}
   * @return  the {@link MessageStreamImpl} object
   */
  @Override
  public <K, V, M extends MessageEnvelope<K, V>> OutputStream<M> createOutStream(StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
    if (!this.outStreams.containsKey(streamSpec.getId())) {
      this.outStreams.putIfAbsent(streamSpec.getId(), new OutputStreamImpl<K, V, M>(this, streamSpec, keySerde, msgSerde));
    }
    return this.outStreams.get(streamSpec.getId());
  }

  /**
   * Helper method to be used by {@link MessageStreamImpl} class
   *
   * @param streamSpec  the {@link StreamSpec} object defining the {@link SystemStream} as an intermediate {@link SystemStream}
   * @param <M>  the type of {@link MessageEnvelope}s in the output {@link SystemStream}
   * @return  the {@link MessageStreamImpl} object
   */
  @Override
  public <K, V, M extends MessageEnvelope<K, V>> OutputStream<M> createIntStream(StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
    if (!this.inStreams.containsKey(streamSpec.getId())) {
      this.inStreams.putIfAbsent(streamSpec.getId(), new IntermediateStreamImpl<K, K, V, M>(this, streamSpec, keySerde, msgSerde));
    }
    IntermediateStreamImpl<K, K, V, M> intStream = (IntermediateStreamImpl<K, K, V, M>) this.inStreams.get(streamSpec.getId());
    if (!this.outStreams.containsKey(streamSpec.getId())) {
      this.outStreams.putIfAbsent(streamSpec.getId(), intStream);
    }
    return intStream;
  }

  @Override public Map<StreamSpec, MessageStream> getInStreams() {
    Map<StreamSpec, MessageStream> inStreamMap = new HashMap<>();
    this.inStreams.forEach((ss, entry) -> inStreamMap.put(((InputStreamImpl) entry).getSpec(), entry));
    return Collections.unmodifiableMap(inStreamMap);
  }

  @Override public Map<StreamSpec, OutputStream> getOutStreams() {
    Map<StreamSpec, OutputStream> outStreamMap = new HashMap<>();
    this.outStreams.forEach((ss, entry) -> {
        StreamSpec streamSpec = (entry instanceof IntermediateStreamImpl) ?
          ((IntermediateStreamImpl) entry).getSpec() :
          ((OutputStreamImpl) entry).getSpec();
        outStreamMap.put(streamSpec, entry);
      });
    return Collections.unmodifiableMap(outStreamMap);
  }

  @Override
  public StreamGraph withContextManager(ContextManager manager) {
    this.contextManager = manager;
    return this;
  }

  public int getNextOpId() {
    return this.opId++;
  }

  public ContextManager getContextManager() {
    return this.contextManager;
  }

  /**
   * Helper method to be get the input stream via {@link SystemStream}
   *
   * @param sstream  the {@link SystemStream}
   * @return  a {@link MessageStreamImpl} object corresponding to the {@code systemStream}
   */
  public MessageStreamImpl getInputStream(SystemStream sstream) {
    for (MessageStream entry: this.inStreams.values()) {
      if (((InputStreamImpl) entry).getSpec().getSystemName() == sstream.getSystem() &&
          ((InputStreamImpl) entry).getSpec().getPhysicalName() == sstream.getStream()) {
        return (MessageStreamImpl) entry;
      }
    }
    return null;
  }

  <M> OutputStream<M> getOutputStream(MessageStreamImpl<M> intStream) {
    if (this.outStreams.containsValue(intStream)) {
      return (OutputStream<M>) intStream;
    }
    return null;
  }

  /**
   * Method to create intermediate topics for {@link MessageStreamImpl#partitionBy(Function)} method.
   *
   * @param parKeyFn  the function to extract the partition key from the input message
   * @param <PK>  the type of partition key
   * @param <M>  the type of input message
   * @return  the {@link OutputStream} object for the re-partitioned stream
   */
  <PK, M> MessageStreamImpl<M> createIntStream(String streamId, Function<M, PK> parKeyFn) {
    StreamSpec streamSpec = executionEnvironment.streamFromConfig(streamId);

    if (!this.inStreams.containsKey(streamSpec.getId())) {
      this.inStreams.putIfAbsent(streamSpec.getId(), new IntermediateStreamImpl(this, streamSpec, null, null, parKeyFn));
    }
    IntermediateStreamImpl intStream = (IntermediateStreamImpl) this.inStreams.get(streamSpec.getId());
    if (!this.outStreams.containsKey(streamSpec.getId())) {
      this.outStreams.putIfAbsent(streamSpec.getId(), intStream);
    }
    return intStream;
  }
}
