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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowPane;

import java.util.Collection;
import java.util.function.Function;


/**
 * Represents a stream of messages.
 * <p>
 * A {@link MessageStream} can be transformed into another {@link MessageStream} by applying the transforms in this API.
 *
 * @param <M>  type of messages in this stream
 */
@InterfaceStability.Unstable
public interface MessageStream<M> {

  /**
   * Applies the provided 1:1 {@link Function} to messages in this {@link MessageStream} and returns the
   * transformed {@link MessageStream}.
   *
   * @param mapFn the function to transform a message to another message
   * @param <TM> the type of messages in the transformed {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  <TM> MessageStream<TM> map(MapFunction<M, TM> mapFn);

  /**
   * Applies the provided 1:n {@link Function} to transform a message in this {@link MessageStream}
   * to n messages in the transformed {@link MessageStream}
   *
   * @param flatMapFn the function to transform a message to zero or more messages
   * @param <TM> the type of messages in the transformed {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  <TM> MessageStream<TM> flatMap(FlatMapFunction<M, TM> flatMapFn);

  /**
   * Applies the provided {@link Function} to messages in this {@link MessageStream} and returns the
   * transformed {@link MessageStream}.
   * <p>
   * The {@link Function} is a predicate which determines whether a message in this {@link MessageStream}
   * should be retained in the transformed {@link MessageStream}.
   *
   * @param filterFn the predicate to filter messages from this {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  MessageStream<M> filter(FilterFunction<M> filterFn);

  /**
   * Allows sending messages in this {@link MessageStream} to an output using the provided {@link SinkFunction}.
   *
   * NOTE: the output may not be a {@link org.apache.samza.system.SystemStream}. It can be an external database, etc.
   *
   * @param sinkFn  the function to send messages in this stream to output
   */
  void sink(SinkFunction<M> sinkFn);

  /**
   * Allows sending messages in this {@link MessageStream} to an output {@link MessageStream}.
   *
   * NOTE: the {@code stream} has to be a {@link MessageStream}.
   *
   * @param stream  the output {@link MessageStream}
   */
  void sendTo(OutputStream<M> stream);

  /**
   * Allows sending messages to an intermediate {@link MessageStream}.
   *
   * NOTE: the {@code stream} has to be a {@link MessageStream}.
   *
   * @param stream  the intermediate {@link MessageStream} to send the message to
   * @return  the intermediate {@link MessageStream} to consume the messages sent
   */
  MessageStream<M> sendThrough(OutputStream<M> stream);

  /**
   * Groups the messages in this {@link MessageStream} according to the provided {@link Window} semantics
   * (e.g. tumbling, sliding or session windows) and returns the transformed {@link MessageStream} of
   * {@link WindowPane}s.
   * <p>
   * Use the {@link org.apache.samza.operators.windows.Windows} helper methods to create the appropriate windows.
   *
   * @param window the window to group and process messages from this {@link MessageStream}
   * @param <K> the type of key in the message in this {@link MessageStream}. If a key is specified,
   *            panes are emitted per-key.
   * @param <WV> the type of value in the {@link WindowPane} in the transformed {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  <K, WV> MessageStream<WindowPane<K, WV>> window(Window<M, K, WV> window);

  /**
   * Joins this {@link MessageStream} with another {@link MessageStream} using the provided pairwise {@link JoinFunction}.
   * <p>
   * We currently only support 2-way joins.
   *
   * @param otherStream the other {@link MessageStream} to be joined with
   * @param joinFn the function to join messages from this and the other {@link MessageStream}
   * @param <K> the type of join key
   * @param <OM> the type of messages in the other stream
   * @param <RM> the type of messages resulting from the {@code joinFn}
   * @return the joined {@link MessageStream}
   */
  <K, OM, RM> MessageStream<RM> join(MessageStream<OM> otherStream, JoinFunction<K, M, OM, RM> joinFn);

  /**
   * Merge all {@code otherStreams} with this {@link MessageStream}.
   * <p>
   * The merging streams must have the same messages of type {@code M}.
   *
   * @param otherStreams  other {@link MessageStream}s to be merged with this {@link MessageStream}
   * @return  the merged {@link MessageStream}
   */
  MessageStream<M> merge(Collection<MessageStream<M>> otherStreams);

  /**
   * Send the input message to an output {@link org.apache.samza.system.SystemStream} and consume it as input {@link MessageStream} again.
   *
   * Note: this is an transform function only used in logic DAG. In a physical DAG, this is either translated to a NOOP function, or a {@code MessageStream#sendThrough} function.
   *
   * @param parKeyExtractor  a {@link Function} that extract the partition key from a message in this {@link MessageStream}
   * @param <K>  the type of partition key
   * @return  a {@link MessageStream} object after the re-partition
   */
  <K> MessageStream<M> partitionBy(Function<M, K> parKeyExtractor);
}
