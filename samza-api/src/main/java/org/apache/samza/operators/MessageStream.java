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
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.internal.Operators;
import org.apache.samza.operators.internal.Operators.Operator;
import org.apache.samza.operators.internal.WindowOutput;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;


/**
 * This class defines either the input or output streams to/from the operators. Users use the API methods defined here to
 * directly program the stream processing stages that processes a stream and generate another one.
 *
 * @param <M>  Type of message in this stream
 */
@InterfaceStability.Unstable
public class MessageStream<M extends Message> {

  private final Set<Operator> subscribers = new HashSet<>();

  /**
   * Helper method to get the corresponding list of subscribers to a specific {@link MessageStream}.
   *
   * NOTE: This is purely an internal API and should not be used directly by programmers.
   *
   * @return A unmodifiable set containing all {@link Operator}s that subscribe to this {@link MessageStream} object
   */
  public Collection<Operator> getSubscribers() {
    return Collections.unmodifiableSet(this.subscribers);
  }

  /**
   * Public API methods start here
   */

  /**
   * Defines a function API that takes three input parameters w/ types {@code A}, {@code B}, and {@code C} and w/o a return value
   *
   * @param <A>  the type of input {@code a}
   * @param <B>  the type of input {@code b}
   * @param <C>  the type of input {@code c}
   */
  @FunctionalInterface
  public interface VoidFunction3<A, B, C> {
    public void apply(A a, B b, C c);
  }

  /**
   * Method to apply a map function (1:1) on a {@link MessageStream}
   *
   * @param mapper  the mapper function to map one input {@link Message} to one output {@link Message}
   * @param <OM>  the type of the output {@link Message} in the output {@link MessageStream}
   * @return the output {@link MessageStream} by applying the map function on the input {@link MessageStream}
   */
  public <OM extends Message> MessageStream<OM> map(Function<M, OM> mapper) {
    Operator<OM> op = Operators.<M, OM>getStreamOperator(m -> new ArrayList<OM>() { {
        OM r = mapper.apply(m);
        if (r != null) {
          this.add(r);
        }
      } });
    this.subscribers.add(op);
    return op.getOutputStream();
  }

  /**
   * Method to apply a flatMap function (1:n) on a {@link MessageStream}
   *
   * @param flatMapper  the flat mapper function to map one input {@link Message} to zero or more output {@link Message}s
   * @param <OM>  the type of the output {@link Message} in the output {@link MessageStream}
   * @return the output {@link MessageStream} by applying the map function on the input {@link MessageStream}
   */
  public <OM extends Message> MessageStream<OM> flatMap(Function<M, Collection<OM>> flatMapper) {
    Operator<OM> op = Operators.getStreamOperator(flatMapper);
    this.subscribers.add(op);
    return op.getOutputStream();
  }

  /**
   * Method to apply a filter function on a {@link MessageStream}
   *
   * @param filter  the filter function to filter input {@link Message}s from the input {@link MessageStream}
   * @return the output {@link MessageStream} after applying the filter function on the input {@link MessageStream}
   */
  public MessageStream<M> filter(Function<M, Boolean> filter) {
    Operator<M> op = Operators.<M, M>getStreamOperator(t -> new ArrayList<M>() { {
        if (filter.apply(t)) {
          this.add(t);
        }
      } });
    this.subscribers.add(op);
    return op.getOutputStream();
  }

  /**
   * Method to send an input {@link MessageStream} to an output {@link org.apache.samza.system.SystemStream}, and allows the output {@link MessageStream}
   * to be consumed by downstream stream operators again.
   *
   * @param sink  the user-defined sink function to send the input {@link Message}s to the external output systems
   */
  public void sink(VoidFunction3<M, MessageCollector, TaskCoordinator> sink) {
    this.subscribers.add(Operators.getSinkOperator(sink));
  }

  /**
   * Method to perform a window function (i.e. a group-by, aggregate function) on a {@link MessageStream}
   *
   * @param window  the window function to group and aggregate the input {@link Message}s from the input {@link MessageStream}
   * @param <WK>  the type of key in the output {@link Message} from the {@link Windows.Window} function
   * @param <WV>  the type of output value from
   * @param <WS>  the type of window state kept in the {@link Windows.Window} function
   * @param <WM>  the type of {@link org.apache.samza.operators.internal.WindowOutput} message from the {@link Windows.Window} function
   * @return the output {@link MessageStream} after applying the window function on the input {@link MessageStream}
   */
  public <WK, WV, WS extends WindowState<WV>, WM extends WindowOutput<WK, WV>> MessageStream<WM> window(Windows.Window<M, WK, WV, WM> window) {
    Operator<WM> wndOp = Operators.getWindowOperator(Windows.getInternalWindowFn(window));
    this.subscribers.add(wndOp);
    return wndOp.getOutputStream();
  }

  /**
   * Method to add an input {@link MessageStream} to a join function. Note that we currently only support 2-way joins.
   *
   * @param other  the other stream to be joined w/
   * @param merger  the common function to merge messages from this {@link MessageStream} and {@code other}
   * @param <K>  the type of join key
   * @param <JM>  the type of message in the {@link Message} from the other join stream
   * @param <RM>  the type of message in the {@link Message} from the join function
   * @return the output {@link MessageStream} from the join function {@code joiner}
   */
  public <K, JM extends Message<K, ?>, RM extends Message> MessageStream<RM> join(MessageStream<JM> other,
      BiFunction<M, JM, RM> merger) {
    MessageStream<RM> outputStream = new MessageStream<>();

    BiFunction<M, JM, RM> parJoin1 = merger::apply;
    BiFunction<JM, M, RM> parJoin2 = (m, t1) -> merger.apply(t1, m);

    // TODO: need to add default store functions for the two partial join functions

    other.subscribers.add(Operators.<JM, K, M, RM>getPartialJoinOperator(parJoin2, outputStream));
    this.subscribers.add(Operators.<M, K, JM, RM>getPartialJoinOperator(parJoin1, outputStream));
    return outputStream;
  }

  /**
   * Method to merge all {@code others} streams w/ this {@link MessageStream}. The merging streams must have the same type {@code M}
   *
   * @param others  other streams to be merged w/ this one
   * @return  the merged output stream
   */
  public MessageStream<M> merge(Collection<MessageStream<M>> others) {
    MessageStream<M> outputStream = new MessageStream<>();

    others.add(this);
    others.forEach(other -> other.subscribers.add(Operators.getMergeOperator(outputStream)));
    return outputStream;
  }

}
