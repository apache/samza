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

import java.time.Duration;
import java.util.Collection;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.operators.spec.BroadcastOperatorSpec;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec.OpCode;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.spec.PartitionByOperatorSpec;
import org.apache.samza.operators.spec.SendToTableOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;


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
   * The {@link StreamGraphSpec} that contains this {@link MessageStreamImpl}
   */
  private final StreamGraphSpec graph;

  /**
   * The {@link OperatorSpec} associated with this {@link MessageStreamImpl}
   */
  private final OperatorSpec operatorSpec;

  public MessageStreamImpl(StreamGraphSpec graph, OperatorSpec<?, M> operatorSpec) {
    this.graph = graph;
    this.operatorSpec = operatorSpec;
  }

  @Override
  public <TM> MessageStream<TM> map(MapFunction<? super M, ? extends TM> mapFn) {
    String opId = this.graph.getNextOpId(OpCode.MAP);
    StreamOperatorSpec<M, TM> op = OperatorSpecs.createMapOperatorSpec(mapFn, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
    return new MessageStreamImpl<>(this.graph, op);
  }

  @Override
  public MessageStream<M> filter(FilterFunction<? super M> filterFn) {
    String opId = this.graph.getNextOpId(OpCode.FILTER);
    StreamOperatorSpec<M, M> op = OperatorSpecs.createFilterOperatorSpec(filterFn, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
    return new MessageStreamImpl<>(this.graph, op);
  }

  @Override
  public <TM> MessageStream<TM> flatMap(FlatMapFunction<? super M, ? extends TM> flatMapFn) {
    String opId = this.graph.getNextOpId(OpCode.FLAT_MAP);
    StreamOperatorSpec<M, TM> op = OperatorSpecs.createFlatMapOperatorSpec(flatMapFn, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
    return new MessageStreamImpl<>(this.graph, op);
  }

  @Override
  public void sink(SinkFunction<? super M> sinkFn) {
    String opId = this.graph.getNextOpId(OpCode.SINK);
    SinkOperatorSpec<M> op = OperatorSpecs.createSinkOperatorSpec(sinkFn, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
  }

  @Override
  public void sendTo(OutputStream<M> outputStream) {
    String opId = this.graph.getNextOpId(OpCode.SEND_TO);
    OutputOperatorSpec<M> op = OperatorSpecs.createSendToOperatorSpec(
        (OutputStreamImpl<M>) outputStream, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
  }

  @Override
  public <K, WV> MessageStream<WindowPane<K, WV>> window(Window<M, K, WV> window, String userDefinedId) {
    String opId = this.graph.getNextOpId(OpCode.WINDOW, userDefinedId);
    OperatorSpec<M, WindowPane<K, WV>> op = OperatorSpecs.createWindowOperatorSpec((WindowInternal<M, K, WV>) window, opId);
    this.operatorSpec.registerNextOperatorSpec(op);
    return new MessageStreamImpl<>(this.graph, op);
  }

  @Override
  public <K, OM, JM> MessageStream<JM> join(MessageStream<OM> otherStream,
      JoinFunction<? extends K, ? super M, ? super OM, ? extends JM> joinFn,
      Serde<K> keySerde, Serde<M> messageSerde, Serde<OM> otherMessageSerde,
      Duration ttl, String userDefinedId) {
    if (otherStream.equals(this)) throw new SamzaException("Cannot join a MessageStream with itself.");
    String opId = this.graph.getNextOpId(OpCode.JOIN, userDefinedId);
    OperatorSpec<?, OM> otherOpSpec = ((MessageStreamImpl<OM>) otherStream).getOperatorSpec();
    JoinOperatorSpec<K, M, OM, JM> op =
        OperatorSpecs.createJoinOperatorSpec(this.operatorSpec, otherOpSpec, (JoinFunction<K, M, OM, JM>) joinFn, keySerde,
            messageSerde, otherMessageSerde, ttl.toMillis(), opId);
    this.operatorSpec.registerNextOperatorSpec(op);
    otherOpSpec.registerNextOperatorSpec((OperatorSpec<OM, ?>) op);

    return new MessageStreamImpl<>(this.graph, op);
  }

  @Override
  public <K, R extends KV, JM> MessageStream<JM> join(Table<R> table,
      StreamTableJoinFunction<? extends K, ? super M, ? super R, ? extends JM> joinFn) {
    String opId = this.graph.getNextOpId(OpCode.JOIN);
    TableSpec tableSpec = ((TableImpl) table).getTableSpec();
    StreamTableJoinOperatorSpec<K, M, R, JM> joinOpSpec = OperatorSpecs.createStreamTableJoinOperatorSpec(
        tableSpec, (StreamTableJoinFunction<K, M, R, JM>) joinFn, opId);
    this.operatorSpec.registerNextOperatorSpec(joinOpSpec);
    return new MessageStreamImpl<>(this.graph, joinOpSpec);
  }

  @Override
  public MessageStream<M> merge(Collection<? extends MessageStream<? extends M>> otherStreams) {
    if (otherStreams.isEmpty()) return this;
    String opId = this.graph.getNextOpId(OpCode.MERGE);
    StreamOperatorSpec<M, M> op = OperatorSpecs.createMergeOperatorSpec(opId);
    this.operatorSpec.registerNextOperatorSpec(op);
    otherStreams.forEach(other -> ((MessageStreamImpl<M>) other).getOperatorSpec().registerNextOperatorSpec(op));
    return new MessageStreamImpl<>(this.graph, op);
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
    PartitionByOperatorSpec<M, K, V> partitionByOperatorSpec = OperatorSpecs.createPartitionByOperatorSpec(
        intermediateStream.getOutputStream(), keyExtractor, valueExtractor, opId);
    this.operatorSpec.registerNextOperatorSpec(partitionByOperatorSpec);
    return intermediateStream;
  }

  @Override
  public <K, V> MessageStream<KV<K, V>> partitionBy(MapFunction<? super M, ? extends K> keyExtractor,
      MapFunction<? super M, ? extends V> valueExtractor, String userDefinedId) {
    return partitionBy(keyExtractor, valueExtractor, null, userDefinedId);
  }

  @Override
  public <K, V> void sendTo(Table<KV<K, V>> table) {
    String opId = this.graph.getNextOpId(OpCode.SEND_TO);
    SendToTableOperatorSpec<K, V> op =
        OperatorSpecs.createSendToTableOperatorSpec(((TableImpl) table).getTableSpec(), opId);
    this.operatorSpec.registerNextOperatorSpec(op);
  }

  @Override
  public MessageStream<M> broadcast(Serde<M> serde, String userDefinedId) {
    String opId = this.graph.getNextOpId(OpCode.BROADCAST, userDefinedId);
    IntermediateMessageStreamImpl<M> intermediateStream = this.graph.getIntermediateStream(opId, serde, true);
    BroadcastOperatorSpec<M> broadcastOperatorSpec =
        OperatorSpecs.createBroadCastOperatorSpec(intermediateStream.getOutputStream(), opId);
    this.operatorSpec.registerNextOperatorSpec(broadcastOperatorSpec);
    return intermediateStream;
  }

  @Override
  public MessageStream<M> broadcast(String userDefinedId) {
    return broadcast(null, userDefinedId);
  }

  /**
   * Get the {@link OperatorSpec} associated with this {@link MessageStreamImpl}.
   * @return the {@link OperatorSpec} associated with this {@link MessageStreamImpl}.
   */
  protected OperatorSpec<?, M> getOperatorSpec() {
    return this.operatorSpec;
  }

}
