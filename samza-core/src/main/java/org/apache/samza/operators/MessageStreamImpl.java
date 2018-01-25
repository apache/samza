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

import java.io.IOException;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;

import java.time.Duration;
import java.util.Collection;


/**
 * The {@link MessageStream} implementation that lets users describe their logical DAG.
 * Users can obtain an instance by calling {@link StreamGraph#getInputStream}.
 * <p>
 * Each {@link MessageStreamImpl} is associated with a single {@link OperatorSpec} in the DAG and allows
 * users to chain further operators on its {@link OperatorSpec}. In other words, a {@link MessageStreamImpl}
 * presents an "edge-centric" (streams) view of the "node-centric" (specs) logical DAG for the users.
 *
 * @param <M>  type of messages in this {@link MessageStream}
 */
public class MessageStreamImpl<M> implements MessageStream<M> {
  /**
   * The {@link StreamGraphImpl} that contains this {@link MessageStreamImpl}
   */
  private final StreamGraphImpl graph;

  /**
   * The {@link OperatorSpec} associated with this {@link MessageStreamImpl}
   */
  private final OperatorSpec source;

  public MessageStreamImpl(StreamGraphImpl graph, OperatorSpec<?, M> source) {
    this.graph = graph;
    this.source = source;
  }

  @Override
  public <TM> MessageStream<TM> map(MapFunction<? super M, ? extends TM> mapFn) {
    try {
      OperatorSpec<M, TM> op = OperatorSpecs.createMapOperatorSpec(mapFn, this.graph.getNextOpId(OpCode.MAP));
      this.source.registerNextOperatorSpec(op);
      return new MessageStreamImpl<>(this.graph, op);
    } catch (IOException e) {
      throw new SamzaException("Failed to serialize map operator spec.", e);
    }
  }

  @Override
  public MessageStream<M> filter(FilterFunction<? super M> filterFn) {
    try {
      OperatorSpec<M, M> op = OperatorSpecs.createFilterOperatorSpec(filterFn, this.graph.getNextOpId(OpCode.FILTER));
      this.source.registerNextOperatorSpec(op);
      return new MessageStreamImpl<>(this.graph, op);
    } catch (IOException e) {
      throw new SamzaException("Failed to serialize filter operator spec.", e);
    }
  }

  @Override
  public <TM> MessageStream<TM> flatMap(FlatMapFunction<? super M, ? extends TM> flatMapFn) {
    try {
      OperatorSpec<M, TM> op = OperatorSpecs.createFlatMapOperatorSpec(flatMapFn, this.graph.getNextOpId(OpCode.FLAT_MAP));
      this.source.registerNextOperatorSpec(op);
      return new MessageStreamImpl<>(this.graph, op);
    } catch (IOException e) {
      throw new SamzaException("Failed to serialize flatMap operator spec.", e);
    }
  }

  @Override
  public void sink(SinkFunction<? super M> sinkFn) {
    try {
      OperatorSpec<M, Void> op = OperatorSpecs.createSinkOperatorSpec(sinkFn, this.graph.getNextOpId(OpCode.SINK));
      this.source.registerNextOperatorSpec(op);
    } catch (IOException e) {
      throw new SamzaException("Failed to serialize sink operator spec.", e);
    }
  }

  @Override
  public void sendTo(OutputStream<M> outputStream) {
    try {
      OperatorSpec<M, Void> op = OperatorSpecs.createSendToOperatorSpec(
          (OutputStreamImpl<M>) outputStream, this.graph.getNextOpId(OpCode.SEND_TO));
      this.source.registerNextOperatorSpec(op);
    } catch (IOException e) {
      throw new SamzaException("Failed to serialize sendTo operator spec.", e);
    }
  }

  @Override
  public <K, WV> MessageStream<WindowPane<K, WV>> window(Window<M, K, WV> window, String userDefinedId) {
    try {
      OperatorSpec<M, WindowPane<K, WV>> op = OperatorSpecs.createWindowOperatorSpec((WindowInternal<M, K, WV>) window,
          this.graph.getNextOpId(OpCode.WINDOW, userDefinedId));
      this.source.registerNextOperatorSpec(op);
      return new MessageStreamImpl<>(this.graph, op);
    } catch (IOException e) {
      throw new SamzaException("Failed to serialize window operator spec.", e);
    }
  }

  @Override
  public <K, OM, JM> MessageStream<JM> join(MessageStream<OM> otherStream,
      JoinFunction<? extends K, ? super M, ? super OM, ? extends JM> joinFn,
      Serde<K> keySerde, Serde<M> messageSerde, Serde<OM> otherMessageSerde,
      Duration ttl, String userDefinedId) {
    if (otherStream.equals(this)) throw new SamzaException("Cannot join a MessageStream with itself.");
    OperatorSpec<?, OM> otherOpSpec = ((MessageStreamImpl<OM>) otherStream).getSource();
    try {
      OperatorSpec<?, JM> op =
          OperatorSpecs.createJoinOperatorSpec(this.source, otherOpSpec, (JoinFunction<K, M, OM, JM>) joinFn, keySerde,
              messageSerde, otherMessageSerde, ttl.toMillis(), this.graph.getNextOpId(OpCode.JOIN, userDefinedId));
      this.source.registerNextOperatorSpec(op);
      otherOpSpec.registerNextOperatorSpec((OperatorSpec<OM, ?>) op);

      return new MessageStreamImpl<>(this.graph, op);
    } catch (IOException e) {
      throw new SamzaException("Failed to serialize join operator spec.", e);
    }
  }

  @Override
  public MessageStream<M> merge(Collection<? extends MessageStream<? extends M>> otherStreams) {
    if (otherStreams.isEmpty()) return this;
    try {
      OperatorSpec<M, M> op = OperatorSpecs.createMergeOperatorSpec(this.graph.getNextOpId(OpCode.MERGE));
      this.source.registerNextOperatorSpec(op);
      otherStreams.forEach(other -> ((MessageStreamImpl<M>) other).getSource().registerNextOperatorSpec(op));
      return new MessageStreamImpl<>(this.graph, op);
    } catch (IOException e) {
      throw new SamzaException("Failed to serialize merge operator spec.", e);
    }
  }

  @Override
  public <K, V> MessageStream<KV<K, V>> partitionBy(MapFunction<? super M, ? extends K> keyExtractor,
      MapFunction<? super M, ? extends V> valueExtractor, KVSerde<K, V> serde, String userDefinedId) {
    String opId = this.graph.getNextOpId(OpCode.PARTITION_BY, userDefinedId);
    IntermediateMessageStreamImpl<KV<K, V>> intermediateStream = this.graph.getIntermediateStream(opId, serde);
    if (!intermediateStream.isKeyed()) {
      // this can only happen when the default serde partitionBy variant is being used
      throw new SamzaException("partitionBy can not be used with a default serde that is not a KVSerde.");
    }

    try {
      OperatorSpec<M, Void> partitionByOperatorSpec = OperatorSpecs.createPartitionByOperatorSpec(
          intermediateStream.getOutputStream(), keyExtractor, valueExtractor, opId);
      this.source.registerNextOperatorSpec(partitionByOperatorSpec);
      return intermediateStream;
    } catch (IOException e) {
      throw new SamzaException("Failed in serializing OutputStreamImpl for intermediate stream " + opId, e);
    }
  }

  @Override
  public <K, V> MessageStream<KV<K, V>> partitionBy(MapFunction<? super M, ? extends K> keyExtractor,
      MapFunction<? super M, ? extends V> valueExtractor, String userDefinedId) {
    return partitionBy(keyExtractor, valueExtractor, null, userDefinedId);
  }

  /**
   * Get the {@link OperatorSpec} associated with this {@link MessageStreamImpl}.
   * @return the {@link OperatorSpec} associated with this {@link MessageStreamImpl}.
   */
  protected OperatorSpec<?, M> getSource() {
    return this.source;
  }

}
