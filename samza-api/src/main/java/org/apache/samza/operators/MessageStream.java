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
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowPane;

import java.util.Collection;


/**
 * Represents a stream of {@link MessageEnvelope}s.
 * <p>
 * A {@link MessageStream} can be transformed into another {@link MessageStream} by applying the transforms in this API.
 *
 * @param <M>  type of {@link MessageEnvelope}s in this stream
 */
@InterfaceStability.Unstable
public interface MessageStream<M extends MessageEnvelope> {

  /**
   * Applies the provided 1:1 {@link MapFunction} to {@link MessageEnvelope}s in this {@link MessageStream} and returns the
   * transformed {@link MessageStream}.
   *
   * @param mapFn the function to transform a {@link MessageEnvelope} to another {@link MessageEnvelope}
   * @param <TM> the type of {@link MessageEnvelope}s in the transformed {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  <TM extends MessageEnvelope> MessageStream<TM> map(MapFunction<M, TM> mapFn);

  /**
   * Applies the provided 1:n {@link FlatMapFunction} to transform a {@link MessageEnvelope} in this {@link MessageStream}
   * to n {@link MessageEnvelope}s in the transformed {@link MessageStream}
   *
   * @param flatMapFn the function to transform a {@link MessageEnvelope} to zero or more {@link MessageEnvelope}s
   * @param <TM> the type of {@link MessageEnvelope}s in the transformed {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  <TM extends MessageEnvelope> MessageStream<TM> flatMap(FlatMapFunction<M, TM> flatMapFn);

  /**
   * Applies the provided {@link FilterFunction} to {@link MessageEnvelope}s in this {@link MessageStream} and returns the
   * transformed {@link MessageStream}.
   * <p>
   * The {@link FilterFunction} is a predicate which determines whether a {@link MessageEnvelope} in this {@link MessageStream}
   * should be retained in the transformed {@link MessageStream}.
   *
   * @param filterFn the predicate to filter {@link MessageEnvelope}s from this {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  MessageStream<M> filter(FilterFunction<M> filterFn);

  /**
   * Allows sending {@link MessageEnvelope}s in this {@link MessageStream} to an output
   * {@link org.apache.samza.system.SystemStream} using the provided {@link SinkFunction}.
   *
   * @param sinkFn the function to send {@link MessageEnvelope}s in this stream to output systems
   */
  void sink(SinkFunction<M> sinkFn);

  /**
   * Groups and processes the {@link MessageEnvelope}s in this {@link MessageStream} according to the provided {@link Window}
   * (e.g. tumbling, sliding or session windows) and returns the transformed {@link MessageStream} of
   * {@link WindowPane}s.
   * <p>
   * Use the {@link org.apache.samza.operators.windows.Windows} helper methods to create the appropriate windows.
   *
   * @param window the window to group and process {@link MessageEnvelope}s from this {@link MessageStream}
   * @param <K> the type of key in the {@link MessageEnvelope} in this {@link MessageStream}. If a key is specified,
   *            panes are emitted per-key.
   * @param <WK> the type of key in the {@link WindowPane} in the transformed {@link MessageStream}
   * @param <WV> the type of value in the {@link WindowPane} in the transformed {@link MessageStream}
   * @param <WM> the type of {@link WindowPane} in the transformed {@link MessageStream}
   * @return the transformed {@link MessageStream}
   */
  <K, WK, WV, WM extends WindowPane<WK, WV>> MessageStream<WM> window(Window<M, K, WK, WV, WM> window);

  /**
   * Joins this {@link MessageStream} with another {@link MessageStream} using the provided pairwise {@link JoinFunction}.
   * <p>
   * We currently only support 2-way joins.
   *
   * @param otherStream the other {@link MessageStream} to be joined with
   * @param joinFn the function to join {@link MessageEnvelope}s from this and the other {@link MessageStream}
   * @param <K> the type of join key
   * @param <OM> the type of {@link MessageEnvelope}s in the other stream
   * @param <RM> the type of {@link MessageEnvelope}s resulting from the {@code joinFn}
   * @return the joined {@link MessageStream}
   */
  <K, OM extends MessageEnvelope<K, ?>, RM extends MessageEnvelope> MessageStream<RM> join(MessageStream<OM> otherStream,
      JoinFunction<M, OM, RM> joinFn);

  /**
   * Merge all {@code otherStreams} with this {@link MessageStream}.
   * <p>
   * The merging streams must have the same {@link MessageEnvelope} type {@code M}.
   *
   * @param otherStreams  other {@link MessageStream}s to be merged with this {@link MessageStream}
   * @return  the merged {@link MessageStream}
   */
  MessageStream<M> merge(Collection<MessageStream<M>> otherStreams);
  
}
