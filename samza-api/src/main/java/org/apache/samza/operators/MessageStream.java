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
 * A stream of messages that can be transformed into another {@link MessageStream}.
 * <p>
 * A {@link MessageStream} corresponding to an input stream can be obtained using {@link StreamGraph#getInputStream}.
 *
 * @param <M> the type of messages in this stream
 */
@InterfaceStability.Unstable
public interface MessageStream<M> {

  /**
   * Applies the provided 1:1 function to messages in this {@link MessageStream} and returns the
   * transformed {@link MessageStream}.
   *
   * @param mapFn the function to transform a message to another message
   * @param <OM> the type of messages in the transformed {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  <OM> MessageStream<OM> map(MapFunction<? super M, ? extends OM> mapFn);

  /**
   * Applies the provided 1:n function to transform a message in this {@link MessageStream}
   * to n messages in the transformed {@link MessageStream}
   *
   * @param flatMapFn the function to transform a message to zero or more messages
   * @param <OM> the type of messages in the transformed {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  <OM> MessageStream<OM> flatMap(FlatMapFunction<? super M, ? extends OM> flatMapFn);

  /**
   * Applies the provided function to messages in this {@link MessageStream} and returns the
   * filtered {@link MessageStream}.
   * <p>
   * The {@link Function} is a predicate which determines whether a message in this {@link MessageStream}
   * should be retained in the filtered {@link MessageStream}.
   *
   * @param filterFn the predicate to filter messages from this {@link MessageStream}.
   * @return the transformed {@link MessageStream}
   */
  MessageStream<M> filter(FilterFunction<? super M> filterFn);

  /**
   * Allows sending messages in this {@link MessageStream} to an output system using the provided {@link SinkFunction}.
   * <p>
   * Offers more control over processing and sending messages than {@link #sendTo(OutputStream)} since
   * the {@link SinkFunction} has access to the {@link org.apache.samza.task.MessageCollector} and
   * {@link org.apache.samza.task.TaskCoordinator}.
   * <p>
   * This can also be used to send output to a system (e.g. a database) that doesn't have a corresponding
   * Samza SystemProducer implementation.
   *
   * @param sinkFn the function to send messages in this stream to an external system
   */
  void sink(SinkFunction<? super M> sinkFn);

  /**
   * Allows sending messages in this {@link MessageStream} to an {@link OutputStream}.
   *
   * @param outputStream the output stream to send messages to
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   */
  <K, V> void sendTo(OutputStream<K, V, M> outputStream);

  /**
   * Groups the messages in this {@link MessageStream} according to the provided {@link Window} semantics
   * (e.g. tumbling, sliding or session windows) and returns the transformed {@link MessageStream} of
   * {@link WindowPane}s.
   * <p>
   * Use the {@link org.apache.samza.operators.windows.Windows} helper methods to create the appropriate windows.
   * <p>
   * <b>Note:</b> As of version 0.13.0, messages in windows are kept in memory and may be lost in case of failures.
   *
   * @param window the window to group and process messages from this {@link MessageStream}
   * @param <K> the type of key in the message in this {@link MessageStream}. If a key is specified,
   *            panes are emitted per-key.
   * @param <WV> the type of value in the {@link WindowPane} in the transformed {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  <K, WV> MessageStream<WindowPane<K, WV>> window(Window<M, K, WV> window);

  /**
   * Joins this {@link MessageStream} with another {@link MessageStream} using the provided
   * pairwise {@link JoinFunction}.
   * <p>
   * Messages in each stream are retained for the provided {@code ttl} and join results are
   * emitted as matches are found.
   * <p>
   * <b>Note:</b> As of version 0.13.0, messages in joins are kept in memory and may be lost in case of failures.
   *
   * @param otherStream the other {@link MessageStream} to be joined with
   * @param joinFn the function to join messages from this and the other {@link MessageStream}
   * @param ttl the ttl for messages in each stream
   * @param <K> the type of join key
   * @param <JM> the type of messages in the other stream
   * @param <OM> the type of messages resulting from the {@code joinFn}
   * @return the joined {@link MessageStream}
   */
  <K, JM, OM> MessageStream<OM> join(MessageStream<JM> otherStream,
      JoinFunction<? extends K, ? super M, ? super JM, ? extends OM> joinFn, Duration ttl);

  /**
   * Merges all {@code otherStreams} with this {@link MessageStream}.
   *
   * @param otherStreams other {@link MessageStream}s to be merged with this {@link MessageStream}
   * @return the merged {@link MessageStream}
   */
  MessageStream<M> merge(Collection<? extends MessageStream<? extends M>> otherStreams);

  /**
   * Sends the messages of type {@code M}in this {@link MessageStream} to a repartitioned output stream and consumes
   * them as an input {@link MessageStream} again. Uses keys returned by the {@code keyExtractor} as the partition key.
   * <p>
   * <b>Note</b>: Repartitioned streams are created automatically in the default system. The key and message Serdes
   * configured for the default system must be able to serialize and deserialize types K and M respectively.
   *
   * @param keyExtractor the {@link Function} to extract the output message key and partition key from
   *                     the input message
   * @param <K> the type of output message key and partition key
   * @return the repartitioned {@link MessageStream}
   */
  <K> MessageStream<M> partitionBy(Function<? super M, ? extends K> keyExtractor);

}
