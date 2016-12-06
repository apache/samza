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

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.operators.windows.BaseWindowFunction;

import java.util.ArrayList;
import java.util.UUID;
import java.util.function.BiFunction;


/**
 * Factory methods for creating {@link OperatorSpec} instances.
 */
public class OperatorSpecs {

  private OperatorSpecs() {}

  private static String getOperatorId() {
    // TODO: need to change the IDs to be a consistent, durable IDs that can be recovered across container and job restarts
    return UUID.randomUUID().toString();
  }

  /**
   * Creates a {@link StreamOperatorSpec}.
   *
   * @param transformFn  the transformation function
   * @param <M>  type of input {@link MessageEnvelope}
   * @param <OM>  type of output {@link MessageEnvelope}
   * @return  the {@link StreamOperatorSpec}
   */
  public static <M extends MessageEnvelope, OM extends MessageEnvelope> StreamOperatorSpec<M, OM> createStreamOperator(
      FlatMapFunction<M, OM> transformFn) {
    return new StreamOperatorSpec<>(transformFn);
  }

  /**
   * Creates a {@link SinkOperatorSpec}.
   *
   * @param sinkFn  the sink function
   * @param <M>  type of input {@link MessageEnvelope}
   * @return  the {@link SinkOperatorSpec}
   */
  public static <M extends MessageEnvelope> SinkOperatorSpec<M> createSinkOperator(SinkFunction<M> sinkFn) {
    return new SinkOperatorSpec<>(sinkFn);
  }


  /**
   * Creates a {@link WindowOperatorSpec}.
   *
   * @param windowFn the definition for this window that specifies everything about this {@link org.apache.samza.operators.windows.WindowFunction}.
   * @param <M> the type of input {@link MessageEnvelope}
   * @param <K> the type of key in the {@link MessageEnvelope} in this {@link org.apache.samza.operators.MessageStream}. If a key is specified,
   *           results are emitted per-key
   * @param <WK> the type of key in the {@link WindowOutput}
   * @param <WV> the type of value in the window
   * @param <WM> the type of output {@link WindowOutput}
   * @return  the {@link WindowOperatorSpec}
   */

  public static <M extends MessageEnvelope, K, WK, WV, WM extends WindowOutput<WK, WV>> WindowOperatorSpec<M, K, WK, WV, WM> createWindowOperator(BaseWindowFunction<M, K, WV> windowFn) {
    return new WindowOperatorSpec<>(windowFn, OperatorSpecs.getOperatorId());
  }

  /**
   * Creates a {@link PartialJoinOperatorSpec}.
   *
   * @param partialJoinFn  the join function
   * @param joinOutput  the output {@link MessageStreamImpl}
   * @param <M>  type of input {@link MessageEnvelope}
   * @param <K>  type of join key
   * @param <JM>  the type of {@link MessageEnvelope} in the other join stream
   * @param <OM>  the type of {@link MessageEnvelope} in the join output
   * @return  the {@link PartialJoinOperatorSpec}
   */
  public static <M extends MessageEnvelope<K, ?>, K, JM extends MessageEnvelope<K, ?>, OM extends MessageEnvelope> PartialJoinOperatorSpec<M, K, JM, OM> createPartialJoinOperator(
      BiFunction<M, JM, OM> partialJoinFn, MessageStreamImpl<OM> joinOutput) {
    return new PartialJoinOperatorSpec<>(partialJoinFn, joinOutput, OperatorSpecs.getOperatorId());
  }

  /**
   * Creates a {@link StreamOperatorSpec} with a merger function.
   *
   * @param mergeOutput  the output {@link MessageStreamImpl} from the merger
   * @param <M>  the type of input {@link MessageEnvelope}
   * @return  the {@link StreamOperatorSpec} for the merge
   */
  public static <M extends MessageEnvelope> StreamOperatorSpec<M, M> createMergeOperator(MessageStreamImpl<M> mergeOutput) {
    return new StreamOperatorSpec<M, M>(t ->
      new ArrayList<M>() { {
          this.add(t);
        } },
      mergeOutput);
  }
}
