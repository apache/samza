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
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
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
   * @param opId  the unique ID of the operator
   * @param <M>  type of input message
   * @param <OM>  type of output message
   * @return  the {@link StreamOperatorSpec}
   */
  public static <M, OM> StreamOperatorSpec<M, OM> createMapOperatorSpec(
      MapFunction<? super M, ? extends OM> mapFn, int opId) {
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

      @Override
      public void close() {
        mapFn.close();
      }
    }, OperatorSpec.OpCode.MAP, opId);
  }

  /**
   * Creates a {@link StreamOperatorSpec} for {@link FilterFunction}
   *
   * @param filterFn  the transformation function
   * @param opId  the unique ID of the operator
   * @param <M>  type of input message
   * @return  the {@link StreamOperatorSpec}
   */
  public static <M> StreamOperatorSpec<M, M> createFilterOperatorSpec(
      FilterFunction<? super M> filterFn, int opId) {
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

      @Override
      public void close() {
        filterFn.close();
      }
    }, OperatorSpec.OpCode.FILTER, opId);
  }

  /**
   * Creates a {@link StreamOperatorSpec} for {@link FlatMapFunction}.
   *
   * @param flatMapFn  the transformation function
   * @param opId  the unique ID of the operator
   * @param <M>  type of input message
   * @param <OM>  type of output message
   * @return  the {@link StreamOperatorSpec}
   */
  public static <M, OM> StreamOperatorSpec<M, OM> createFlatMapOperatorSpec(
      FlatMapFunction<? super M, ? extends OM> flatMapFn, int opId) {
    return new StreamOperatorSpec<>((FlatMapFunction<M, OM>) flatMapFn, OperatorSpec.OpCode.FLAT_MAP, opId);
  }

  /**
   * Creates a {@link SinkOperatorSpec} for the sink operator.
   *
   * @param sinkFn  the sink function provided by the user
   * @param opId  the unique ID of the operator
   * @param <M>  type of input message
   * @return  the {@link SinkOperatorSpec} for the sink operator
   */
  public static <M> SinkOperatorSpec<M> createSinkOperatorSpec(SinkFunction<? super M> sinkFn, int opId) {
    return new SinkOperatorSpec<>((SinkFunction<M>) sinkFn, opId);
  }

  /**
   * Creates a {@link OutputOperatorSpec} for the sendTo operator.
   *
   * @param outputStream  the {@link OutputStreamImpl} to send messages to
   * @param opId  the unique ID of the operator
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param <M> the type of message in the {@link OutputStreamImpl}
   * @return  the {@link OutputOperatorSpec} for the sendTo operator
   */
  public static <K, V, M> OutputOperatorSpec<M> createSendToOperatorSpec(
      OutputStreamImpl<K, V, M> outputStream, int opId) {
    return new OutputOperatorSpec<>(outputStream, OperatorSpec.OpCode.SEND_TO, opId);
  }

  /**
   * Creates a {@link OutputOperatorSpec} for the partitionBy operator.
   *
   * @param outputStream  the {@link OutputStreamImpl} to send messages to
   * @param opId  the unique ID of the operator
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param <M> the type of message in the {@link OutputStreamImpl}
   * @return  the {@link OutputOperatorSpec} for the partitionBy operator
   */
  public static <K, V, M> OutputOperatorSpec<M> createPartitionByOperatorSpec(
      OutputStreamImpl<K, V, M> outputStream, int opId) {
    return new OutputOperatorSpec<>(outputStream, OperatorSpec.OpCode.PARTITION_BY, opId);
  }

  /**
   * Creates a {@link WindowOperatorSpec}.
   *
   * @param window  the description of the window.
   * @param opId  the unique ID of the operator
   * @param <M>  the type of input message
   * @param <WK>  the type of key in the window output
   * @param <WV>  the type of value in the window
   * @return  the {@link WindowOperatorSpec}
   */

  public static <M, WK, WV> WindowOperatorSpec<M, WK, WV> createWindowOperatorSpec(
      WindowInternal<M, WK, WV> window, int opId) {
    return new WindowOperatorSpec<>(window, opId);
  }

  /**
   * Creates a {@link JoinOperatorSpec}.
   *
   * @param leftInputOpSpec  the operator spec for the stream on the left side of the join
   * @param rightInputOpSpec  the operator spec for the stream on the right side of the join
   * @param joinFn  the user-defined join function to get join keys and results
   * @param ttlMs  the ttl in ms for retaining messages in each stream
   * @param opId  the unique ID of the operator
   * @param <K>  the type of join key
   * @param <M>  the type of input message
   * @param <JM>  the type of message in the other join stream
   * @param <RM>  the type of join result
   * @return  the {@link JoinOperatorSpec}
   */
  public static <K, M, JM, RM> JoinOperatorSpec<K, M, JM, RM> createJoinOperatorSpec(
      OperatorSpec<?, M> leftInputOpSpec, OperatorSpec<?, JM> rightInputOpSpec,
      JoinFunction<K, M, JM, RM> joinFn, long ttlMs, int opId) {
    return new JoinOperatorSpec<>(leftInputOpSpec, rightInputOpSpec, joinFn, ttlMs, opId);
  }

  /**
   * Creates a {@link StreamOperatorSpec} with a merger function.
   *
   * @param opId  the unique ID of the operator
   * @param <M>  the type of input message
   * @return  the {@link StreamOperatorSpec} for the merge
   */
  public static <M> StreamOperatorSpec<M, M> createMergeOperatorSpec(int opId) {
    return new StreamOperatorSpec<>(message ->
        new ArrayList<M>() {
          {
            this.add(message);
          }
        },
        OperatorSpec.OpCode.MERGE, opId);
  }
}
