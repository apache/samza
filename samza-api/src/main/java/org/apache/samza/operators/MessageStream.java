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
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;


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
   * The {@link FilterFunction} is a predicate which determines whether a message in this {@link MessageStream}
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
   */
  void sendTo(OutputStream<M> outputStream);

  /**
   * Groups the messages in this {@link MessageStream} according to the provided {@link Window} semantics
   * (e.g. tumbling, sliding or session windows) and returns the transformed {@link MessageStream} of
   * {@link WindowPane}s.
   * <p>
   * Use the {@link org.apache.samza.operators.windows.Windows} helper methods to create the appropriate windows.
   * <p>
   * <b>Warning:</b> As of version 0.13.0, messages in windows are kept in memory and will be lost during restarts.
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
   * Both inputs being joined must have the same number of partitions, and should be partitioned by the join key.
   * <p>
   * <b>Warning:</b> As of version 0.13.0, messages in joins are kept in memory and will be lost during restarts.
   *
   * @param otherStream the other {@link MessageStream} to be joined with
   * @param joinFn the function to join messages from this and the other {@link MessageStream}
   * @param ttl the ttl for messages in each stream
   * @param keySerde the serde for the join key
   * @param messageSerde the serde for messages in this stream
   * @param otherMessageSerde the serde for messages in the other stream
   * @param <K> the type of join key
   * @param <OM> the type of messages in the other stream
   * @param <JM> the type of messages resulting from the {@code joinFn}
   * @return the joined {@link MessageStream}
   */
  <K, OM, JM> MessageStream<JM> join(MessageStream<OM> otherStream,
      JoinFunction<? extends K, ? super M, ? super OM, ? extends JM> joinFn,
      Serde<K> keySerde, Serde<M> messageSerde, Serde<OM> otherMessageSerde, Duration ttl);

  /**
   * Joins this {@link MessageStream} with another {@link RecordTable} using the provided
   * pairwise {@link StreamTableJoinFunction}.
   * <p>
   * Messages are looked up from the joined table, join function is applied and join results are
   * emitted as matches are found.
   * <p>
   * Both the input stream and table being joined must have the same number of partitions,
   * and should be partitioned by the join key.
   * <p>
   *
   * @param table the table being joined
   * @param joinFn the join function
   * @param <K> the type of the join key
   * @param <M> the type of messages from the stream
   * @param <R> the type of record in the table
   * @param <JM> the type of messages resulting from the {@code joinFn}
   * @return the joined {@link MessageStream}
   */
  <K, M, R, JM> MessageStream<JM> join(RecordTable<K, R> table,
      StreamTableJoinFunction<? extends K, ? super M, ? super R, ? extends JM> joinFn);

  /**
   * Merges all {@code otherStreams} with this {@link MessageStream}.
   * <p>
   * The merged stream contains messages from all streams in the order they arrive.
   *
   * @param otherStreams other {@link MessageStream}s to be merged with this {@link MessageStream}
   * @return the merged {@link MessageStream}
   */
  MessageStream<M> merge(Collection<? extends MessageStream<? extends M>> otherStreams);

  /**
   * Merges all {@code streams}.
   * <p>
   * The merged {@link MessageStream} contains messages from all {@code streams} in the order they arrive.
   *
   * @param streams {@link MessageStream}s to be merged
   * @param <T> the type of messages in each of the streams
   * @return the merged {@link MessageStream}
   * @throws IllegalArgumentException if {@code streams} is empty
   */
  static <T> MessageStream<T> mergeAll(Collection<? extends MessageStream<? extends T>> streams) {
    if (streams.isEmpty()) {
      throw new IllegalArgumentException("No streams to merge.");
    }
    ArrayList<MessageStream<T>> messageStreams = new ArrayList<>((Collection<MessageStream<T>>) streams);
    MessageStream<T> firstStream = messageStreams.remove(0);
    return firstStream.merge(messageStreams);
  }

  /**
   * Re-partitions this {@link MessageStream} using keys from the {@code keyExtractor} by creating a new
   * intermediate stream on the {@code job.default.system}. This intermediate stream is both an output and
   * input to the job.
   * <p>
   * Uses the provided {@link KVSerde} for serialization of keys and values. If the provided {@code serde} is null,
   * uses the default serde provided via {@link StreamGraph#setDefaultSerde}, which must be a KVSerde.
   * If no default serde has been provided <b>before</b> calling this method, no-op serdes are used for keys and values.
   * <p>
   * The number of partitions for this intermediate stream is determined as follows:
   * If the stream is an eventual input to a {@link #join}, and the number of partitions for the other stream is known,
   * then number of partitions for this stream is set to the number of partitions in the other input stream.
   * Else, the number of partitions is set to the value of the {@code job.intermediate.stream.partitions}
   * configuration, if present.
   * Else, the number of partitions is set to to the max of number of partitions for all input and output streams
   * (excluding intermediate streams).
   *
   * @param <K> the type of output key
   * @param <V> the type of output value
   * @param keyExtractor the {@link Function} to extract the message and partition key from the input message
   * @param valueExtractor the {@link Function} to extract the value from the input message
   * @param serde the {@link KVSerde} to use for (de)serializing the key and value.
   * @return the repartitioned {@link MessageStream}
   */
  <K, V> MessageStream<KV<K, V>> partitionBy(Function<? super M, ? extends K> keyExtractor,
      Function<? super M, ? extends V> valueExtractor, KVSerde<K, V> serde);

  /**
   * Same as calling {@link #partitionBy(Function, Function, KVSerde)} with a null KVSerde.
   *
   * @param keyExtractor the {@link Function} to extract the message and partition key from the input message
   * @param valueExtractor the {@link Function} to extract the value from the input message
   * @param <K> the type of output key
   * @param <V> the type of output value
   * @return the repartitioned {@link MessageStream}
   */
  <K, V> MessageStream<KV<K, V>> partitionBy(Function<? super M, ? extends K> keyExtractor,
      Function<? super M, ? extends V> valueExtractor);

  /**
   * Allows writing messages in this {@link MessageStream} to an {@link RecordTable}.
   *
   * @param table the table to write messages to
   * @param keyExtractor the {@link Function} to extract the key from the input message
   * @param valueExtractor the {@link Function} to extract the value from the input message
   * @param <K> the type of key in the table
   * @param <V> the type of record in the table
   */
  <K, V> void writeTo(
      RecordTable<K, V> table,
      Function<? super M, ? extends K> keyExtractor,
      Function<? super M, ? extends V> valueExtractor);

}
