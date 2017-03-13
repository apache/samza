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

import java.util.Collection;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;

import java.util.ArrayList;
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
   * @param graph  the {@link StreamGraphImpl} object
   * @param output  the output {@link MessageStreamImpl} object
   * @param <M>  type of input message
   * @param <OM>  type of output message
   * @return  the {@link StreamOperatorSpec}
   */
  public static <M, OM> StreamOperatorSpec<M, OM> createMapOperatorSpec(MapFunction<M, OM> mapFn, StreamGraphImpl graph, MessageStreamImpl<OM> output) {
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
    }, output, OperatorSpec.OpCode.MAP, graph.getNextOpId());
  }

  /**
   * Creates a {@link StreamOperatorSpec} for {@link FilterFunction}
   *
   * @param filterFn  the transformation function
   * @param graph  the {@link StreamGraphImpl} object
   * @param output  the output {@link MessageStreamImpl} object
   * @param <M>  type of input message
   * @return  the {@link StreamOperatorSpec}
   */
  public static <M> StreamOperatorSpec<M, M> createFilterOperatorSpec(FilterFunction<M> filterFn, StreamGraphImpl graph, MessageStreamImpl<M> output) {
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
    }, output, OperatorSpec.OpCode.FILTER, graph.getNextOpId());
  }

  /**
   * Creates a {@link StreamOperatorSpec}.
   *
   * @param transformFn  the transformation function
   * @param graph  the {@link StreamGraphImpl} object
   * @param output  the output {@link MessageStreamImpl} object
   * @param <M>  type of input message
   * @param <OM>  type of output message
   * @return  the {@link StreamOperatorSpec}
   */
  public static <M, OM> StreamOperatorSpec<M, OM> createStreamOperatorSpec(
      FlatMapFunction<M, OM> transformFn, StreamGraphImpl graph, MessageStreamImpl<OM> output) {
    return new StreamOperatorSpec<>(transformFn, output, OperatorSpec.OpCode.FLAT_MAP, graph.getNextOpId());
  }

  /**
   * Creates a {@link SinkOperatorSpec}.
   *
   * @param sinkFn  the sink function
   * @param <M>  type of input message
   * @param graph  the {@link StreamGraphImpl} object
   * @return  the {@link SinkOperatorSpec}
   */
  public static <M> SinkOperatorSpec<M> createSinkOperatorSpec(SinkFunction<M> sinkFn, StreamGraphImpl graph) {
    return new SinkOperatorSpec<>(sinkFn, OperatorSpec.OpCode.SINK, graph.getNextOpId());
  }

  /**
   * Creates a {@link SinkOperatorSpec}.
   *
   * @param sinkFn  the sink function
   * @param graph  the {@link StreamGraphImpl} object
   * @param stream  the {@link OutputStream} where the message is sent to
   * @param <M>  type of input message
   * @return  the {@link SinkOperatorSpec}
   */
  public static <M> SinkOperatorSpec<M> createSendToOperatorSpec(SinkFunction<M> sinkFn, StreamGraphImpl graph, OutputStream<M> stream) {
    return new SinkOperatorSpec<>(sinkFn, OperatorSpec.OpCode.SEND_TO, graph.getNextOpId(), stream);
  }

  /**
   * Creates a {@link SinkOperatorSpec}.
   *
   * @param sinkFn  the sink function
   * @param stream  the {@link OutputStream} where the message is sent to
   * @param opId operator ID
   * @param <M>  type of input message
   * @return  the {@link SinkOperatorSpec}
   */
  public static <M> SinkOperatorSpec<M> createPartitionOperatorSpec(SinkFunction<M> sinkFn, OutputStream<M> stream, int opId) {
    return new SinkOperatorSpec<>(sinkFn, OperatorSpec.OpCode.PARTITION_BY, opId, stream);
  }

  /**
   * Creates a {@link WindowOperatorSpec}.
   *
   * @param window the description of the window.
   * @param graph  the {@link StreamGraphImpl} object
   * @param wndOutput  the window output {@link MessageStreamImpl} object
   * @param <M> the type of input message
   * @param <WK> the type of key in the {@link WindowPane}
   * @param <WV> the type of value in the window
   * @return  the {@link WindowOperatorSpec}
   */

  public static <M, WK, WV> WindowOperatorSpec<M, WK, WV> createWindowOperatorSpec(
      WindowInternal<M, WK, WV> window, StreamGraphImpl graph, MessageStreamImpl<WindowPane<WK, WV>> wndOutput) {
    return new WindowOperatorSpec<>(window, wndOutput, graph.getNextOpId());
  }

  /**
   * Creates a {@link PartialJoinOperatorSpec}.
   *
   * @param partialJoinFn  the join function
   * @param graph  the {@link StreamGraphImpl} object
   * @param joinOutput  the output {@link MessageStreamImpl}
   * @param <M>  type of input message
   * @param <K>  type of join key
   * @param <JM>  the type of message in the other join stream
   * @param <OM>  the type of message in the join output
   * @return  the {@link PartialJoinOperatorSpec}
   */
  public static <M, K, JM, OM> PartialJoinOperatorSpec<M, K, JM, OM> createPartialJoinOperatorSpec(
      PartialJoinFunction<K, M, JM, OM> partialJoinFn, StreamGraphImpl graph, MessageStreamImpl<OM> joinOutput) {
    return new PartialJoinOperatorSpec<>(partialJoinFn, joinOutput, graph.getNextOpId());
  }

  /**
   * Creates a {@link StreamOperatorSpec} with a merger function.
   *
   * @param graph  the {@link StreamGraphImpl} object
   * @param mergeOutput  the output {@link MessageStreamImpl} from the merger
   * @param <M>  the type of input message
   * @return  the {@link StreamOperatorSpec} for the merge
   */
  public static <M> StreamOperatorSpec<M, M> createMergeOperatorSpec(StreamGraphImpl graph, MessageStreamImpl<M> mergeOutput) {
    return new StreamOperatorSpec<M, M>(message ->
        new ArrayList<M>() {
          {
            this.add(message);
          }
        },
        mergeOutput, OperatorSpec.OpCode.MERGE, graph.getNextOpId());
  }
}
