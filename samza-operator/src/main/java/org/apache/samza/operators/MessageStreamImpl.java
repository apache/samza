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

import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowFn;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.operators.windows.WindowState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;


/**
 * The implementation for input/output streams to/from the operators. Users use the {@link MessageStream} API methods
 * to describe and chain the operators specs in the stream.
 *
 * @param <M>  type of message in this stream
 */
public class MessageStreamImpl<M extends Message> implements MessageStream<M> {

  /**
   * The set of operators that consume the output from this message stream.
   */
  private final Set<OperatorSpec> registeredOperatorSpecs = new HashSet<>();

  @Override
  public <OM extends Message> MessageStream<OM> map(MapFunction<M, OM> mapFn) {
    OperatorSpec<OM> op = OperatorSpecs.<M, OM>createStreamOperator(m -> new ArrayList<OM>() { {
        OM r = mapFn.apply(m);
        if (r != null) {
          this.add(r);
        }
      } });
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override
  public <OM extends Message> MessageStream<OM> flatMap(FlatMapFunction<M, OM> flatMapFn) {
    OperatorSpec<OM> op = OperatorSpecs.createStreamOperator(flatMapFn);
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override
  public MessageStream<M> filter(FilterFunction<M> filterFn) {
    OperatorSpec<M> op = OperatorSpecs.<M, M>createStreamOperator(t -> new ArrayList<M>() { {
        if (filterFn.apply(t)) {
          this.add(t);
        }
      } });
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override
  public void sink(SinkFunction<M> sinkFn) {
    this.registeredOperatorSpecs.add(OperatorSpecs.createSinkOperator(sinkFn));
  }

  @Override
  public <WK, WV, WS extends WindowState<WV>, WM extends WindowOutput<WK, WV>> MessageStream<WM> window(
      Window<M, WK, WV, WM> window) {
    OperatorSpec<WM> wndOp = OperatorSpecs.createWindowOperator((WindowFn<M, WK, WS, WM>) window.getInternalWindowFn());
    this.registeredOperatorSpecs.add(wndOp);
    return wndOp.getOutputStream();
  }

  @Override
  public <K, JM extends Message<K, ?>, RM extends Message> MessageStream<RM> join(MessageStream<JM> otherStream,
      JoinFunction<M, JM, RM> joinFn) {
    MessageStreamImpl<RM> outputStream = new MessageStreamImpl<>();

    BiFunction<M, JM, RM> parJoin1 = joinFn::apply;
    BiFunction<JM, M, RM> parJoin2 = (m, t1) -> joinFn.apply(t1, m);

    // TODO: need to add default store functions for the two partial join functions

    ((MessageStreamImpl<JM>) otherStream).getRegisteredOperatorSpecs().add(
        OperatorSpecs.<JM, K, M, RM>createPartialJoinOperator(parJoin2, outputStream));
    this.getRegisteredOperatorSpecs().add(OperatorSpecs.<M, K, JM, RM>createPartialJoinOperator(parJoin1, outputStream));
    return outputStream;
  }

  @Override
  public MessageStream<M> merge(Collection<MessageStream<M>> otherStreams) {
    MessageStreamImpl<M> outputStream = new MessageStreamImpl<>();

    otherStreams.add(this);
    otherStreams.forEach(other ->
        ((MessageStreamImpl<M>) other)
            .getRegisteredOperatorSpecs()
            .add(OperatorSpecs.createMergeOperator(outputStream)));
    return outputStream;
  }

  /**
   * Gets the operator specs registered to consume the ouput of this MessageStream. This is an internal API and
   * should not be exposed to users.
   *
   * @return  a collection containing all {@link OperatorSpec}s that are registered with this MessageStream.
   */
  public Collection<OperatorSpec> getRegisteredOperatorSpecs() {
    return this.registeredOperatorSpecs;
  }
}
