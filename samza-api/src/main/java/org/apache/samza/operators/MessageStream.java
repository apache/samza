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

import java.time.Duration;
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
   * Applies the provided 1:1 function to messages in this {@link MessageStream} and returns the
   * transformed {@link MessageStream}.
   *
   * @param mapFn the function to transform a message to another message
   * @param <TM> the type of messages in the transformed {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  <TM> MessageStream<TM> map(MapFunction<M, TM> mapFn);

  /**
   * Applies the provided 1:n function to transform a message in this {@link MessageStream}
   * to n messages in the transformed {@link MessageStream}
   *
   * @param flatMapFn the function to transform a message to zero or more messages
   * @param <TM> the type of messages in the transformed {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  <TM> MessageStream<TM> flatMap(FlatMapFunction<M, TM> flatMapFn);

  /**
   * Applies the provided function to messages in this {@link MessageStream} and returns the
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
   * NOTE: If the output is for a {@link org.apache.samza.system.SystemStream}, use
   * {@link #sendTo(String, Function, Function)} instead. This transform should only be used to output to
   * non-stream systems (e.g., an external database).
   *
   * @param sinkFn the function to send messages in this stream to an external system
   */
  void sink(SinkFunction<M> sinkFn);

  /**
   * Allows sending messages in this {@link MessageStream} to an output {@link MessageStream}.
   *
   * @param streamId the unique logical ID for the output stream
   * @param keyExtractor the {@link Function} to extract the key for the outgoing message
   * @param msgExtractor the {@link Function} to extract the message for the outgoing message
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   */
  <K, V> void sendTo(String streamId, Function<M, K> keyExtractor, Function<M, V> msgExtractor);

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
   * Messages in each stream are retained (currently, in memory) for the provided {@code ttl} and join results are
   * emitted as matches are found.
   *
   * @param otherStream the other {@link MessageStream} to be joined with
   * @param joinFn the function to join messages from this and the other {@link MessageStream}
   * @param ttl the ttl for messages in each stream
   * @param <K> the type of join key
   * @param <OM> the type of messages in the other stream
   * @param <RM> the type of messages resulting from the {@code joinFn}
   * @return the joined {@link MessageStream}
   */
  <K, OM, RM> MessageStream<RM> join(MessageStream<OM> otherStream, JoinFunction<K, M, OM, RM> joinFn, Duration ttl);

  /**
   * Merge all {@code otherStreams} with this {@link MessageStream}.
   * <p>
   * The merging streams must have the same messages of type {@code M}.
   *
   * @param otherStreams other {@link MessageStream}s to be merged with this {@link MessageStream}
   * @return the merged {@link MessageStream}
   */
  MessageStream<M> merge(Collection<MessageStream<M>> otherStreams);

  /**
   * Sends the messages in this {@link MessageStream} to a repartitioned output stream and consumes them as
   * an input {@link MessageStream} again. Uses keys returned by the {@code keyExtractor} as the partition key.
   *
   * @param keyExtractor the {@link Function} to extract the output message key and partition key from
   *                     the input message
   * @param <K> the type of output message key and partition key
   * @return the repartitioned {@link MessageStream}
   */
  <K> MessageStream<M> partitionBy(Function<M, K> keyExtractor);

}
