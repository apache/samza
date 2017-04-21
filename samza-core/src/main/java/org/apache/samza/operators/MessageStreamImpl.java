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
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.stream.OutputStreamInternal;
import org.apache.samza.operators.util.InternalInMemoryStore;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;


/**
 * The implementation for input/output {@link MessageStream}s to/from the operators.
 * Users use the {@link MessageStream} API methods to describe and chain the operators specs.
 *
 * @param <M>  type of messages in this {@link MessageStream}
 */
public class MessageStreamImpl<M> implements MessageStream<M> {
  /**
   * The {@link StreamGraphImpl} that contains this {@link MessageStreamImpl}
   */
  private final StreamGraphImpl graph;

  /**
   * The set of operators that consume the messages in this {@link MessageStream}
   */
  private final Set<OperatorSpec> registeredOperatorSpecs = new HashSet<>();

  /**
   * Default constructor
   *
   * @param graph the {@link StreamGraphImpl} object that this stream belongs to
   */
  public MessageStreamImpl(StreamGraphImpl graph) {
    this.graph = graph;
  }

  @Override
  public <TM> MessageStream<TM> map(MapFunction<? super M, ? extends TM> mapFn) {
    OperatorSpec<TM> op = OperatorSpecs.createMapOperatorSpec(
        mapFn, new MessageStreamImpl<>(this.graph), this.graph.getNextOpId());
    this.registeredOperatorSpecs.add(op);
    return op.getNextStream();
  }

  @Override
  public MessageStream<M> filter(FilterFunction<? super M> filterFn) {
    OperatorSpec<M> op = OperatorSpecs.createFilterOperatorSpec(
        filterFn, new MessageStreamImpl<>(this.graph), this.graph.getNextOpId());
    this.registeredOperatorSpecs.add(op);
    return op.getNextStream();
  }

  @Override
  public <TM> MessageStream<TM> flatMap(FlatMapFunction<? super M, ? extends TM> flatMapFn) {
    OperatorSpec<TM> op = OperatorSpecs.createStreamOperatorSpec(
        flatMapFn, new MessageStreamImpl<>(this.graph), this.graph.getNextOpId());
    this.registeredOperatorSpecs.add(op);
    return op.getNextStream();
  }

  @Override
  public void sink(SinkFunction<? super M> sinkFn) {
    SinkOperatorSpec<M> op = OperatorSpecs.createSinkOperatorSpec(sinkFn, this.graph.getNextOpId());
    this.registeredOperatorSpecs.add(op);
  }

  @Override
  public <K, V> void sendTo(OutputStream<K, V, M> outputStream) {
    SinkOperatorSpec<M> op = OperatorSpecs.createSendToOperatorSpec(
        (OutputStreamInternal<K, V, M>) outputStream, this.graph.getNextOpId());
    this.registeredOperatorSpecs.add(op);
  }

  @Override
  public <K, WV> MessageStream<WindowPane<K, WV>> window(Window<M, K, WV> window) {
    OperatorSpec<WindowPane<K, WV>> wndOp = OperatorSpecs.createWindowOperatorSpec(
        (WindowInternal<M, K, WV>) window, new MessageStreamImpl<>(this.graph), this.graph.getNextOpId());
    this.registeredOperatorSpecs.add(wndOp);
    return wndOp.getNextStream();
  }

  @Override
  public <K, OM, TM> MessageStream<TM> join(
      MessageStream<OM> otherStream, JoinFunction<? extends K, ? super M, ? super OM, ? extends TM> joinFn, Duration ttl) {
    MessageStreamImpl<TM> nextStream = new MessageStreamImpl<>(this.graph);

    PartialJoinFunction<K, M, OM, TM> thisPartialJoinFn = new PartialJoinFunction<K, M, OM, TM>() {
      private KeyValueStore<K, PartialJoinFunction.PartialJoinMessage<M>> thisStreamState;

      @Override
      public TM apply(M m, OM jm) {
        return joinFn.apply(m, jm);
      }

      @Override
      public K getKey(M message) {
        return joinFn.getFirstKey(message);
      }

      @Override
      public KeyValueStore<K, PartialJoinMessage<M>> getState() {
        return thisStreamState;
      }

      @Override
      public void init(Config config, TaskContext context) {
        // joinFn#init() must only be called once, so we do it in this partial join function's #init.
        joinFn.init(config, context);

        thisStreamState = new InternalInMemoryStore<>();
      }
    };

    PartialJoinFunction<K, OM, M, TM> otherPartialJoinFn = new PartialJoinFunction<K, OM, M, TM>() {
      private KeyValueStore<K, PartialJoinMessage<OM>> otherStreamState;

      @Override
      public TM apply(OM om, M m) {
        return joinFn.apply(m, om);
      }

      @Override
      public K getKey(OM message) {
        return joinFn.getSecondKey(message);
      }

      @Override
      public KeyValueStore<K, PartialJoinMessage<OM>> getState() {
        return otherStreamState;
      }

      @Override
      public void init(Config config, TaskContext taskContext) {
        otherStreamState = new InternalInMemoryStore<>();
      }
    };

    this.registeredOperatorSpecs.add(OperatorSpecs.createPartialJoinOperatorSpec(
        thisPartialJoinFn, otherPartialJoinFn, ttl.toMillis(), nextStream, this.graph.getNextOpId()));

    ((MessageStreamImpl<OM>) otherStream).registeredOperatorSpecs
        .add(OperatorSpecs.createPartialJoinOperatorSpec(
            otherPartialJoinFn, thisPartialJoinFn, ttl.toMillis(), nextStream, this.graph.getNextOpId()));

    return nextStream;
  }

  @Override
  public MessageStream<M> merge(Collection<MessageStream<? extends M>> otherStreams) {
    MessageStreamImpl<M> nextStream = new MessageStreamImpl<>(this.graph);

    otherStreams.add(this);
    otherStreams.forEach(other -> ((MessageStreamImpl<M>) other).registeredOperatorSpecs.
        add(OperatorSpecs.createMergeOperatorSpec(nextStream, this.graph.getNextOpId())));
    return nextStream;
  }

  @Override
  public <K> MessageStream<M> partitionBy(Function<? super M, ? extends K> keyExtractor) {
    int opId = this.graph.getNextOpId();
    String opName = String.format("%s-%s", OperatorSpec.OpCode.PARTITION_BY.name().toLowerCase(), opId);
    MessageStreamImpl<M> intermediateStream =
        this.graph.<K, M, M>getIntermediateStream(opName, keyExtractor, m -> m, (k, m) -> m);
    SinkOperatorSpec<M> partitionByOperatorSpec = OperatorSpecs.createPartitionByOperatorSpec(
        (OutputStreamInternal<K, M, M>) intermediateStream, opId);
    this.registeredOperatorSpecs.add(partitionByOperatorSpec);
    return intermediateStream;
  }

  /**
   * Gets the operator specs registered to consume the output of this {@link MessageStream}. This is an internal API and
   * should not be exposed to users.
   *
   * @return  a collection containing all {@link OperatorSpec}s that are registered with this {@link MessageStream}.
   */
  public Collection<OperatorSpec> getRegisteredOperatorSpecs() {
    return Collections.unmodifiableSet(this.registeredOperatorSpecs);
  }

}
