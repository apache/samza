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

package org.apache.samza.operators.spec;

import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.stream.OutputStreamInternal;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.task.TaskContext;

import java.util.ArrayList;
import java.util.Collection;


/**
 * Factory methods for creating {@link OperatorSpec} instances.
 */
public class OperatorSpecs {

  private OperatorSpecs() {}

  /**
   * Creates a {@link StreamOperatorSpec} for {@link MapFunction}
   *
   * @param mapFn  the map function
   * @param nextStream  the output {@link MessageStreamImpl} to send messages to
   * @param opId  the unique ID of the operator
   * @param <M>  type of input message
   * @param <OM>  type of output message
   * @return  the {@link StreamOperatorSpec}
   */
  public static <M, OM> StreamOperatorSpec<M, OM> createMapOperatorSpec(
      MapFunction<M, OM> mapFn, MessageStreamImpl<OM> nextStream, int opId) {
    return new StreamOperatorSpec<>(new FlatMapFunction<M, OM>() {
      @Override
      public Collection<OM> apply(M message) {
        return new ArrayList<OM>() {
          {
            OM r = mapFn.apply(message);
            if (r != null) {
              this.add(r);
            }
          }
        };
      }

      @Override
      public void init(Config config, TaskContext context) {
        mapFn.init(config, context);
      }
    }, nextStream, OperatorSpec.OpCode.MAP, opId);
  }

  /**
   * Creates a {@link StreamOperatorSpec} for {@link FilterFunction}
   *
   * @param filterFn  the transformation function
   * @param nextStream  the output {@link MessageStreamImpl} to send messages to
   * @param opId  the unique ID of the operator
   * @param <M>  type of input message
   * @return  the {@link StreamOperatorSpec}
   */
  public static <M> StreamOperatorSpec<M, M> createFilterOperatorSpec(
      FilterFunction<M> filterFn, MessageStreamImpl<M> nextStream, int opId) {
    return new StreamOperatorSpec<>(new FlatMapFunction<M, M>() {
      @Override
      public Collection<M> apply(M message) {
        return new ArrayList<M>() {
          {
            if (filterFn.apply(message)) {
              this.add(message);
            }
          }
        };
      }

      @Override
      public void init(Config config, TaskContext context) {
        filterFn.init(config, context);
      }
    }, nextStream, OperatorSpec.OpCode.FILTER, opId);
  }

  /**
   * Creates a {@link StreamOperatorSpec}.
   *
   * @param transformFn  the transformation function
   * @param nextStream  the output {@link MessageStreamImpl} to send messages to
   * @param opId  the unique ID of the operator
   * @param <M>  type of input message
   * @param <OM>  type of output message
   * @return  the {@link StreamOperatorSpec}
   */
  public static <M, OM> StreamOperatorSpec<M, OM> createStreamOperatorSpec(
      FlatMapFunction<M, OM> transformFn, MessageStreamImpl<OM> nextStream, int opId) {
    return new StreamOperatorSpec<>(transformFn, nextStream, OperatorSpec.OpCode.FLAT_MAP, opId);
  }

  /**
   * Creates a {@link SinkOperatorSpec} for the sink operator.
   *
   * @param sinkFn  the sink function provided by the user
   * @param opId  the unique ID of the operator
   * @param <M>  type of input message
   * @return  the {@link SinkOperatorSpec} for the sink operator
   */
  public static <M> SinkOperatorSpec<M> createSinkOperatorSpec(SinkFunction<M> sinkFn, int opId) {
    return new SinkOperatorSpec<>(sinkFn, OperatorSpec.OpCode.SINK, opId);
  }

  /**
   * Creates a {@link SinkOperatorSpec} for the sendTo operator.
   *
   * @param outputStream  the {@link OutputStreamInternal} to send messages to
   * @param opId  the unique ID of the operator
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param <M> the type of message in the {@link OutputStreamInternal}
   * @return  the {@link SinkOperatorSpec} for the sendTo operator
   */
  public static <K, V, M> SinkOperatorSpec<M> createSendToOperatorSpec(
      OutputStreamInternal<K, V, M> outputStream, int opId) {
    return new SinkOperatorSpec<>(outputStream, OperatorSpec.OpCode.SEND_TO, opId);
  }

  /**
   * Creates a {@link SinkOperatorSpec} for the partitionBy operator.
   *
   * @param outputStream  the {@link OutputStreamInternal} to send messages to
   * @param opId  the unique ID of the operator
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param <M> the type of message in the {@link OutputStreamInternal}
   * @return  the {@link SinkOperatorSpec} for the partitionBy operator
   */
  public static <K, V, M> SinkOperatorSpec<M> createPartitionByOperatorSpec(
      OutputStreamInternal<K, V, M> outputStream, int opId) {
    return new SinkOperatorSpec<>(outputStream, OperatorSpec.OpCode.PARTITION_BY, opId);
  }

  /**
   * Creates a {@link WindowOperatorSpec}.
   *
   * @param window  the description of the window.
   * @param nextStream  the output {@link MessageStreamImpl} to send messages to
   * @param opId  the unique ID of the operator
   * @param <M>  the type of input message
   * @param <WK>  the type of key in the {@link WindowPane}
   * @param <WV>  the type of value in the window
   * @return  the {@link WindowOperatorSpec}
   */

  public static <M, WK, WV> WindowOperatorSpec<M, WK, WV> createWindowOperatorSpec(
      WindowInternal<M, WK, WV> window, MessageStreamImpl<WindowPane<WK, WV>> nextStream, int opId) {
    return new WindowOperatorSpec<>(window, nextStream, opId);
  }

  /**
   * Creates a {@link PartialJoinOperatorSpec}.
   *
   * @param thisPartialJoinFn  the partial join function for this message stream
   * @param otherPartialJoinFn  the partial join function for the other message stream
   * @param ttlMs  the ttl in ms for retaining messages in each stream
   * @param nextStream  the output {@link MessageStreamImpl} to send messages to
   * @param opId  the unique ID of the operator
   * @param <K>  the type of join key
   * @param <M>  the type of input message
   * @param <JM>  the type of message in the other join stream
   * @param <RM>  the type of message in the join output
   * @return  the {@link PartialJoinOperatorSpec}
   */
  public static <K, M, JM, RM> PartialJoinOperatorSpec<K, M, JM, RM> createPartialJoinOperatorSpec(
      PartialJoinFunction<K, M, JM, RM> thisPartialJoinFn, PartialJoinFunction<K, JM, M, RM> otherPartialJoinFn,
      long ttlMs, MessageStreamImpl<RM> nextStream, int opId) {
    return new PartialJoinOperatorSpec<K, M, JM, RM>(thisPartialJoinFn, otherPartialJoinFn, ttlMs, nextStream, opId);
  }

  /**
   * Creates a {@link StreamOperatorSpec} with a merger function.
   *
   * @param nextStream  the output {@link MessageStreamImpl} to send messages to
   * @param opId  the unique ID of the operator
   * @param <M>  the type of input message
   * @return  the {@link StreamOperatorSpec} for the merge
   */
  public static <M> StreamOperatorSpec<M, M> createMergeOperatorSpec(MessageStreamImpl<M> nextStream, int opId) {
    return new StreamOperatorSpec<M, M>(message ->
        new ArrayList<M>() {
          {
            this.add(message);
          }
        },
        nextStream, OperatorSpec.OpCode.MERGE, opId);
  }
}
