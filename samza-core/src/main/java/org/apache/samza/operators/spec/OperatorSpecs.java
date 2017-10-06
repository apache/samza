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

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.TableSpec;
import org.apache.samza.task.TaskContext;


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
   * @param <M> the type of message in the {@link OutputStreamImpl}
   * @return  the {@link OutputOperatorSpec} for the sendTo operator
   */
  public static <M> OutputOperatorSpec<M> createSendToOperatorSpec(OutputStreamImpl<M> outputStream, int opId) {
    return new OutputOperatorSpec<>(outputStream, opId);
  }

  /**
   * Creates a {@link PartitionByOperatorSpec} for the partitionBy operator.
   *
   * @param <M> the type of messages being repartitioned
   * @param <K> the type of key in the repartitioned {@link OutputStreamImpl}
   * @param <V> the type of value in the repartitioned {@link OutputStreamImpl}
   * @param outputStream  the {@link OutputStreamImpl} to send messages to
   * @param keyFunction  the {@link MapFunction} for extracting the key from the message
   * @param valueFunction  the {@link MapFunction} for extracting the value from the message
   * @param opId  the unique ID of the operator
   * @return  the {@link OutputOperatorSpec} for the partitionBy operator
   */
  public static <M, K, V> PartitionByOperatorSpec<M, K, V> createPartitionByOperatorSpec(
      OutputStreamImpl<KV<K, V>> outputStream, Function<? super M, ? extends K> keyFunction,
      Function<? super M, ? extends V> valueFunction, int opId) {
    return new PartitionByOperatorSpec<>(outputStream, keyFunction, valueFunction, opId);
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
   * @param keySerde  the serde for the join key
   * @param messageSerde  the serde for messages in the stream on the lefta side of the join
   * @param otherMessageSerde  the serde for messages in the stream on the right side of the join
   * @param ttlMs  the ttl in ms for retaining messages in each stream
   * @param opId  the unique ID of the operator
   * @param <K>  the type of join key
   * @param <M>  the type of input message
   * @param <OM>  the type of message in the other stream
   * @param <JM>  the type of join result
   * @return  the {@link JoinOperatorSpec}
   */
  public static <K, M, OM, JM> JoinOperatorSpec<K, M, OM, JM> createJoinOperatorSpec(
      OperatorSpec<?, M> leftInputOpSpec, OperatorSpec<?, OM> rightInputOpSpec, JoinFunction<K, M, OM, JM> joinFn,
      Serde<K> keySerde, Serde<M> messageSerde, Serde<OM> otherMessageSerde, long ttlMs, int opId) {
    return new JoinOperatorSpec<>(leftInputOpSpec, rightInputOpSpec, joinFn,
        keySerde, messageSerde, otherMessageSerde, ttlMs, opId);
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

  /**
   * Creates a {@link StreamTableJoinOperatorSpec} with a join function.
   *
   * @param leftInputOpSpec the operator spec for the stream on the left side of the join
   * @param tableSpec the table spec for the table on the right side of the join
   * @param joinFn the user-defined join function to get join keys and results
   * @param opId the unique ID of the operator
   * @param <M> the type of the incoming message
   * @param <K> the type of the table record key
   * @param <R> the type of the table record value
   * @param <OM> the type of the join result
   * @return the {@link StreamTableJoinOperatorSpec}
   */
  public static <K, M, R, OM> StreamTableJoinOperatorSpec<K, M, R, OM> createStreamTableJoinOperatorSpec(
      OperatorSpec<?, M> leftInputOpSpec, TableSpec tableSpec,
      StreamTableJoinFunction<K, M, R, OM> joinFn, int opId) {
    return new StreamTableJoinOperatorSpec<>(leftInputOpSpec, tableSpec, joinFn, opId);
  }

  /**
   * Creates a {@link WriteToOperatorSpec} with a key extractor and a value extractor function.
   *
   * @param inputOpSpec the operator spec for the input stream
   * @param tableSpec the table spec for the underlying table
   * @param keyExtractor user-defined function to extract the record key from the incoming message
   * @param valueExtractor user-defined function to extract the record value from the incoming message
   * @param opId the unique ID of the operator
   * @param <M> the type of the incoming message
   * @param <K> the type of the table record key
   * @param <V> the type of the table record value
   * @return the {@link WriteToOperatorSpec}
   */
  public static <K, V, M> WriteToOperatorSpec<K, V, M> createWriteToOperatorSpec(
      OperatorSpec<?, M> inputOpSpec, TableSpec tableSpec,
      Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> valueExtractor, int opId) {
    return new WriteToOperatorSpec(inputOpSpec, tableSpec, keyExtractor, valueExtractor, opId);
  }

}
