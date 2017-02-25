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

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import java.util.HashSet;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.util.InternalInMemoryStore;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;


/**
 * The implementation for input/output {@link MessageStream}s to/from the operators.
 * Users use the {@link MessageStream} API methods to describe and chain the operators specs.
 *
 * @param <M>  type of messages in this {@link MessageStream}
 */
public class MessageStreamImpl<M> implements MessageStream<M> {
  /**
   * The {@link StreamGraphImpl} object that contains this {@link MessageStreamImpl}
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
  MessageStreamImpl(StreamGraphImpl graph) {
    this.graph = graph;
  }

  @Override
  public <TM> MessageStream<TM> map(MapFunction<M, TM> mapFn) {
    OperatorSpec<TM> op = OperatorSpecs.<M, TM>createMapOperatorSpec(mapFn, this.graph, new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(op);
    return op.getNextStream();
  }

  @Override
  public MessageStream<M> filter(FilterFunction<M> filterFn) {
    OperatorSpec<M> op = OperatorSpecs.<M>createFilterOperatorSpec(filterFn, this.graph, new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(op);
    return op.getNextStream();
  }

  @Override
  public <TM> MessageStream<TM> flatMap(FlatMapFunction<M, TM> flatMapFn) {
    OperatorSpec<TM> op = OperatorSpecs.createStreamOperatorSpec(flatMapFn, this.graph, new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(op);
    return op.getNextStream();
  }

  @Override
  public void sink(SinkFunction<M> sinkFn) {
    this.registeredOperatorSpecs.add(OperatorSpecs.createSinkOperatorSpec(sinkFn, this.graph));
  }

  @Override
  public void sendTo(OutputStream<M> stream) {
    this.registeredOperatorSpecs.add(OperatorSpecs.createSendToOperatorSpec(stream.getSinkFunction(), this.graph, stream));
  }

  @Override
  public <K, WV> MessageStream<WindowPane<K, WV>> window(Window<M, K, WV> window) {
    OperatorSpec<WindowPane<K, WV>> wndOp = OperatorSpecs.createWindowOperatorSpec((WindowInternal<M, K, WV>) window,
        this.graph, new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(wndOp);
    return wndOp.getNextStream();
  }

  @Override
  public <K, JM, RM> MessageStream<RM> join(MessageStream<JM> otherStream, JoinFunction<K, M, JM, RM> joinFn, long ttlMs) {
    MessageStreamImpl<RM> outputStream = new MessageStreamImpl<>(this.graph);

    PartialJoinFunction<K, M, JM, RM> thisPartialJoinFn = new PartialJoinFunction<K, M, JM, RM>() {
      private KeyValueStore<K, PartialJoinMessage<M>> thisStreamState;

      @Override
      public RM apply(M m, JM jm) {
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

    PartialJoinFunction<K, JM, M, RM> otherPartialJoinFn = new PartialJoinFunction<K, JM, M, RM>() {
      private KeyValueStore<K, PartialJoinMessage<JM>> otherStreamState;

      @Override
      public RM apply(JM om, M m) {
        return joinFn.apply(m, om);
      }

      @Override
      public K getKey(JM message) {
        return joinFn.getSecondKey(message);
      }

      @Override
      public KeyValueStore<K, PartialJoinMessage<JM>> getState() {
        return otherStreamState;
      }

      @Override
      public void init(Config config, TaskContext taskContext) {
        otherStreamState = new InternalInMemoryStore<>();
      }
    };

    this.registeredOperatorSpecs.add(OperatorSpecs.<K, M, JM, RM>createPartialJoinOperatorSpec(
        thisPartialJoinFn, otherPartialJoinFn, ttlMs, this.graph, outputStream));

    ((MessageStreamImpl<JM>) otherStream).registeredOperatorSpecs.add(OperatorSpecs.<K, JM, M, RM>createPartialJoinOperatorSpec(
        otherPartialJoinFn, thisPartialJoinFn, ttlMs, this.graph, outputStream));

    return outputStream;
  }

  @Override
  public MessageStream<M> merge(Collection<MessageStream<M>> otherStreams) {
    MessageStreamImpl<M> outputStream = new MessageStreamImpl<>(this.graph);

    otherStreams.add(this);
    otherStreams.forEach(other -> ((MessageStreamImpl<M>) other).registeredOperatorSpecs.
        add(OperatorSpecs.createMergeOperatorSpec(this.graph, outputStream)));
    return outputStream;
  }

  @Override
  public <K> MessageStream<M> partitionBy(Function<M, K> parKeyExtractor) {
    MessageStreamImpl<M> intStream = this.graph.createIntStream(parKeyExtractor);
    OutputStream<M> outputStream = this.graph.getOutputStream(intStream);
    this.registeredOperatorSpecs.add(OperatorSpecs.createPartitionOperatorSpec(outputStream.getSinkFunction(),
        this.graph, outputStream));
    return intStream;
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
