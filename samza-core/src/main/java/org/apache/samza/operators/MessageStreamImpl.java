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

import org.apache.samza.SamzaException;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.spec.PartitionByOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Function;


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
  private final OperatorSpec operatorSpec;

  public MessageStreamImpl(StreamGraphImpl graph, OperatorSpec<?, M> operatorSpec) {
    this.graph = graph;
    this.operatorSpec = operatorSpec;
  }

  @Override
  public <TM> MessageStream<TM> map(MapFunction<? super M, ? extends TM> mapFn) {
    String opId = this.graph.getNextOpId(OpCode.MAP, null);
    OperatorSpec<M, TM> op = OperatorSpecs.createMapOperatorSpec(mapFn, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
    return new MessageStreamImpl<>(this.graph, op);
  }

  @Override
  public MessageStream<M> filter(FilterFunction<? super M> filterFn) {
    String opId = this.graph.getNextOpId(OpCode.FILTER, null);
    OperatorSpec<M, M> op = OperatorSpecs.createFilterOperatorSpec(filterFn, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
    return new MessageStreamImpl<>(this.graph, op);
  }

  @Override
  public <TM> MessageStream<TM> flatMap(FlatMapFunction<? super M, ? extends TM> flatMapFn) {
    String opId = this.graph.getNextOpId(OpCode.FLAT_MAP, null);
    OperatorSpec<M, TM> op = OperatorSpecs.createFlatMapOperatorSpec(flatMapFn, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
    return new MessageStreamImpl<>(this.graph, op);
  }

  @Override
  public void sink(SinkFunction<? super M> sinkFn) {
    String opId = this.graph.getNextOpId(OpCode.SINK, null);
    SinkOperatorSpec<M> op = OperatorSpecs.createSinkOperatorSpec(sinkFn, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
  }

  @Override
  public void sendTo(OutputStream<M> outputStream) {
    String opId = this.graph.getNextOpId(OpCode.SEND_TO, null);
    OutputOperatorSpec<M> op = OperatorSpecs.createSendToOperatorSpec((OutputStreamImpl<M>) outputStream, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
  }

  @Override
  public <K, WV> MessageStream<WindowPane<K, WV>> window(Window<M, K, WV> window, String userProvidedId) {
    String opId = this.graph.getNextOpId(OpCode.WINDOW, userProvidedId);
    OperatorSpec<M, WindowPane<K, WV>> op = OperatorSpecs.createWindowOperatorSpec(
        (WindowInternal<M, K, WV>) window, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
    return new MessageStreamImpl<>(this.graph, op);
  }

  @Override
  public <K, OM, JM> MessageStream<JM> join(MessageStream<OM> otherStream,
      JoinFunction<? extends K, ? super M, ? super OM, ? extends JM> joinFn,
      Serde<K> keySerde, Serde<M> messageSerde, Serde<OM> otherMessageSerde,
      Duration ttl, String userProvidedId) {
    if (otherStream.equals(this)) throw new SamzaException("Cannot join a MessageStream with itself.");
    OperatorSpec<?, OM> otherOpSpec = ((MessageStreamImpl<OM>) otherStream).getOperatorSpec();
    String opId = this.graph.getNextOpId(OpCode.JOIN, userProvidedId);
    JoinOperatorSpec<K, M, OM, JM> joinOpSpec =
        OperatorSpecs.createJoinOperatorSpec(this.operatorSpec, otherOpSpec, (JoinFunction<K, M, OM, JM>) joinFn,
            keySerde, messageSerde, otherMessageSerde, ttl.toMillis(), opId);

    this.operatorSpec.registerNextOperatorSpec(joinOpSpec);
    otherOpSpec.registerNextOperatorSpec((OperatorSpec<OM, ?>) joinOpSpec);

    return new MessageStreamImpl<>(this.graph, joinOpSpec);
  }

  @Override
  public MessageStream<M> merge(Collection<? extends MessageStream<? extends M>> otherStreams) {
    if (otherStreams.isEmpty()) return this;
    String opId = this.graph.getNextOpId(OpCode.MERGE, null);
    StreamOperatorSpec<M, M> opSpec = OperatorSpecs.createMergeOperatorSpec(opId);
    this.operatorSpec.registerNextOperatorSpec(opSpec);
    otherStreams.forEach(other -> ((MessageStreamImpl<M>) other).getOperatorSpec().registerNextOperatorSpec(opSpec));
    return new MessageStreamImpl<>(this.graph, opSpec);
  }

  @Override
  public <K, V> MessageStream<KV<K, V>> partitionBy(Function<? super M, ? extends K> keyExtractor,
      Function<? super M, ? extends V> valueExtractor, KVSerde<K, V> serde, String userProvidedId) {
    String opId = this.graph.getNextOpId(OpCode.PARTITION_BY, userProvidedId);
    IntermediateMessageStreamImpl<KV<K, V>> intermediateStream = this.graph.getIntermediateStream(opId, serde);
    PartitionByOperatorSpec<M, K, V> partitionByOperatorSpec =
        OperatorSpecs.createPartitionByOperatorSpec(
            intermediateStream.getOutputStream(), keyExtractor, valueExtractor, opId);
    this.operatorSpec.registerNextOperatorSpec(partitionByOperatorSpec);
    return intermediateStream;
  }

  @Override
  public <K, V> MessageStream<KV<K, V>> partitionBy(Function<? super M, ? extends K> keyExtractor,
      Function<? super M, ? extends V> valueExtractor, String userProvidedId) {
    return partitionBy(keyExtractor, valueExtractor, null, userProvidedId);
  }

  /**
   * Get the {@link OperatorSpec} associated with this {@link MessageStreamImpl}.
   * @return the {@link OperatorSpec} associated with this {@link MessageStreamImpl}.
   */
  protected OperatorSpec<?, M> getOperatorSpec() {
    return this.operatorSpec;
  }
}
