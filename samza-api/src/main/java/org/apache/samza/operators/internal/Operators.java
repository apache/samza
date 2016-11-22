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

package org.apache.samza.operators.internal;

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.WindowState;
import org.apache.samza.operators.data.Message;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;


/**
 * This class defines all basic stream operator classes used by internal implementation only. All classes defined in
 * this file are immutable.
 *
 * NOTE: Programmers should not use the operators defined in this class directly. All {@link Operator} objects
 * should be initiated via {@link MessageStream} API methods
 */
public class Operators {
  /**
   * Private constructor to prevent instantiation of the {@link Operators} class
   */
  private Operators() {}

  private static String getOperatorId() {
    // TODO: need to change the IDs to be a consistent, durable IDs that can be recovered across container and job restarts
    return UUID.randomUUID().toString();
  }

  /**
   * Private interface for stream operator functions. The interface class defines the output of the stream operator function.
   *
   */
  public interface Operator<OM extends Message> {
    MessageStream<OM> getOutputStream();
  }

  /**
   * Linear stream operator function that takes 1 input {@link Message} and output a collection of output {@link Message}s.
   *
   * @param <M>  the type of input {@link Message}
   * @param <OM>  the type of output {@link Message}
   */
  public static class StreamOperator<M extends Message, OM extends Message> implements Operator<OM> {
    /**
     * The output {@link MessageStream}
     */
    private final MessageStream<OM> outputStream;

    /**
     * The transformation function
     */
    private final Function<M, Collection<OM>> txfmFunction;

    /**
     * Constructor of {@link StreamOperator}. Make it private s.t. it can only be created within {@link Operators}.
     *
     * @param transformFn  the transformation function to be applied that transforms 1 input {@link Message} into a collection
     *                     of output {@link Message}s
     */
    private StreamOperator(Function<M, Collection<OM>> transformFn) {
      this(transformFn, new MessageStream<>());
    }

    /**
     * Constructor of {@link StreamOperator} which allows the user to define the output {@link MessageStream}
     *
     * @param transformFn  the transformation function
     * @param outputStream  the output {@link MessageStream}
     */
    private StreamOperator(Function<M, Collection<OM>> transformFn, MessageStream<OM> outputStream) {
      this.outputStream = outputStream;
      this.txfmFunction = transformFn;
    }

    @Override
    public MessageStream<OM> getOutputStream() {
      return this.outputStream;
    }

    /**
     * Method to get the transformation function.
     *
     * @return the {@code txfmFunction}
     */
    public Function<M, Collection<OM>> getFunction() {
      return this.txfmFunction;
    }

  }

  /**
   * A sink operator function that allows customized code to send the output to external system. This is the terminal
   * operator that does not have any output {@link MessageStream} that allows further processing in the same
   * {@link org.apache.samza.operators.task.StreamOperatorTask}
   *
   * @param <M>  the type of input {@link Message}
   */
  public static class SinkOperator<M extends Message> implements Operator {

    /**
     * The user-defined sink function
     */
    private final MessageStream.VoidFunction3<M, MessageCollector, TaskCoordinator> sink;

    /**
     * Default constructor for {@link SinkOperator}. Make it private s.t. it can only be created within {@link Operators}.
     *
     * @param sink  the user-defined sink function
     */
    private SinkOperator(MessageStream.VoidFunction3<M, MessageCollector, TaskCoordinator> sink) {
      this.sink = sink;
    }

    @Override
    public MessageStream getOutputStream() {
      return null;
    }

    /**
     * Method to get the user-defined function implements the {@link SinkOperator}
     *
     * @return a {@link MessageStream.VoidFunction3} function that allows the caller to pass in an input message, {@link MessageCollector}
     *         and {@link TaskCoordinator} to the sink function
     */
    public MessageStream.VoidFunction3<M, MessageCollector, TaskCoordinator> getFunction() {
      return this.sink;
    }
  }

  /**
   * The store functions that are used by {@link WindowOperator} and {@link PartialJoinOperator} to store and retrieve
   * buffered messages and partial aggregation results
   *
   * @param <SK>  the type of key used to store the operator states
   * @param <SS>  the type of operator state. e.g. could be the partial aggregation result for a window, or a buffered
   *             input message from the join stream for a join
   */
  public static class StoreFunctions<M extends Message, SK, SS> {
    /**
     * Function to define the key to query in the operator state store, according to the incoming {@link Message}
     * This method only supports finding the unique key for the incoming message, which supports use case of non-overlapping
     * windows and unique-key-based join.
     *
     * TODO: for windows that overlaps (i.e. sliding windows and hopping windows) and non-unique-key-based join, the query
     * to the state store is usually a range scan. We need to add a rangeKeyFinder function to map from a single input
     * message to a range of keys in the store.
     */
    private final Function<M, SK> storeKeyFinder;

    /**
     * Function to update the store entry based on the current state and the incoming {@link Message}
     *
     * TODO: this is assuming a 1:1 mapping from the input message to the store entry. When implementing sliding/hopping
     * windows and non-unique-key-based join, we may need to include the corresponding state key, in addition to the
     * state value.
     */
    private final BiFunction<M, SS, SS> stateUpdater;

    /**
     * Constructor of state store functions.
     *
     */
    private StoreFunctions(Function<M, SK> keyFinder,
        BiFunction<M, SS, SS> stateUpdater) {
      this.storeKeyFinder = keyFinder;
      this.stateUpdater = stateUpdater;
    }

    /**
     * Method to get the {@code storeKeyFinder} function
     *
     * @return  the function to calculate the key from an input {@link Message}
     */
    public Function<M, SK> getStoreKeyFinder() {
      return this.storeKeyFinder;
    }

    /**
     * Method to get the {@code stateUpdater} function
     *
     * @return  the function to update the corresponding state according to an input {@link Message}
     */
    public BiFunction<M, SS, SS> getStateUpdater() {
      return this.stateUpdater;
    }
  }

  /**
   * Defines a window operator function that takes one {@link MessageStream} as an input, accumulate the window state, and generate
   * an output {@link MessageStream} w/ output type {@code WM} which extends {@link WindowOutput}
   *
   * @param <M>  the type of input {@link Message}
   * @param <WK>  the type of key in the output {@link Message} from the {@link WindowOperator} function
   * @param <WS>  the type of window state in the {@link WindowOperator} function
   * @param <WM>  the type of window output {@link Message}
   */
  public static class WindowOperator<M extends Message, WK, WS extends WindowState, WM extends WindowOutput<WK, ?>> implements Operator<WM> {
    /**
     * The output {@link MessageStream}
     */
    private final MessageStream<WM> outputStream;

    /**
     * The main window transformation function that takes {@link Message}s from one input stream, aggregates w/ the window
     * state(s) from the window state store, and generate output {@link Message}s to the output stream.
     */
    private final BiFunction<M, Entry<WK, WS>, WM> txfmFunction;

    /**
     * The state store functions for the {@link WindowOperator}
     */
    private final StoreFunctions<M, WK, WS> storeFunctions;

    /**
     * The window trigger function
     */
    private final Trigger<M, WS> trigger;

    /**
     * The unique ID of stateful operators
     */
    private final String opId;

    /**
     * Constructor for {@link WindowOperator}. Make it private s.t. it can only be created within {@link Operators}.
     *
     * @param windowFn  description of the window function
     * @param operatorId  auto-generated unique ID of the operator
     */
    private WindowOperator(WindowFn<M, WK, WS, WM> windowFn, String operatorId) {
      this.outputStream = new MessageStream<>();
      this.txfmFunction = windowFn.getTransformFunc();
      this.storeFunctions = windowFn.getStoreFuncs();
      this.trigger = windowFn.getTrigger();
      this.opId = operatorId;
    }

    @Override
    public String toString() {
      return this.opId;
    }

    @Override
    public MessageStream<WM> getOutputStream() {
      return this.outputStream;
    }

    /**
     * Method to get the window's {@link StoreFunctions}.
     *
     * @return  the window operator's {@code storeFunctions}
     */
    public StoreFunctions<M, WK, WS> getStoreFunctions() {
      return this.storeFunctions;
    }

    /**
     * Method to get the window operator's main function
     *
     * @return   the window operator's {@code txfmFunction}
     */
    public BiFunction<M, Entry<WK, WS>, WM> getFunction() {
      return this.txfmFunction;
    }

    /**
     * Method to get the trigger functions
     *
     * @return  the {@link Trigger} for this {@link WindowOperator}
     */
    public Trigger<M, WS> getTrigger() {
      return this.trigger;
    }

    /**
     * Method to generate the window operator's state store name
     *
     * @param inputStream the input {@link MessageStream} to this state store
     * @return   the persistent store name of the window operator
     */
    public String getStoreName(MessageStream<M> inputStream) {
      //TODO: need to get the persistent name of ds and the operator in a serialized form
      return String.format("input-%s-wndop-%s", inputStream.toString(), this.toString());
    }
  }

  /**
   * The partial join operator that takes {@link Message}s from one input stream and join w/ buffered {@link Message}s from
   * another stream and generate join output to {@code output}
   *
   * @param <M>  the type of input {@link Message}
   * @param <K>  the type of join key
   * @param <JM>  the type of message of {@link Message} in the other join stream
   * @param <RM>  the type of message of {@link Message} in the join output stream
   */
  public static class PartialJoinOperator<M extends Message<K, ?>, K, JM extends Message<K, ?>, RM extends Message> implements Operator<RM> {

    private final MessageStream<RM> joinOutput;

    /**
     * The main transformation function of {@link PartialJoinOperator} that takes a type {@code M} input message,
     * join w/ a stream of buffered {@link Message}s from another stream w/ type {@code JM}, and generate joined type {@code RM}.
     */
    private final BiFunction<M, JM, RM> txfmFunction;

    /**
     * The message store functions that read the buffered messages from the other stream in the join
     */
    private final StoreFunctions<JM, K, JM> joinStoreFunctions;

    /**
     * The message store functions that save the buffered messages of this {@link MessageStream} in the join
     */
    private final StoreFunctions<M, K, M> selfStoreFunctions;

    /**
     * The unique ID for the stateful operator
     */
    private final String opId;

    /**
     * Default constructor to create a {@link PartialJoinOperator} object
     *
     * @param partialJoin  partial join function that take type {@code M} of input {@link Message} and join w/ type
     *                     {@code JM} of buffered {@link Message} from another stream
     * @param joinOutput  the output {@link MessageStream} of the join results
     */
    private PartialJoinOperator(BiFunction<M, JM, RM> partialJoin, MessageStream<RM> joinOutput, String opId) {
      this.joinOutput = joinOutput;
      this.txfmFunction = partialJoin;
      // Read-only join store, no creator/updater functions specified
      this.joinStoreFunctions = new StoreFunctions<>(m -> m.getKey(), null);
      // Buffered message store for this input stream
      this.selfStoreFunctions = new StoreFunctions<>(m -> m.getKey(), (m, s1) -> m);
      this.opId = opId;
    }

    @Override
    public String toString() {
      return this.opId;
    }

    @Override
    public MessageStream<RM> getOutputStream() {
      return this.joinOutput;
    }

    /**
     * Method to get {@code joinStoreFunctions}
     *
     * @return  {@code joinStoreFunctions}
     */
    public StoreFunctions<JM, K, JM> getJoinStoreFunctions() {
      return this.joinStoreFunctions;
    }

    /**
     * Method to get {@code selfStoreFunctions}
     *
     * @return  {@code selfStoreFunctions}
     */
    public StoreFunctions<M, K, M> getSelfStoreFunctions() {
      return this.selfStoreFunctions;
    }

    /**
     * Method to get {@code txfmFunction}
     *
     * @return  {@code txfmFunction}
     */
    public BiFunction<M, JM, RM> getFunction() {
      return this.txfmFunction;
    }
  }

  /**
   * The method only to be used internally in {@link MessageStream} to create {@link StreamOperator}
   *
   * @param transformFn  the corresponding transformation function
   * @param <M>  type of input {@link Message}
   * @param <OM>  type of output {@link Message}
   * @return  the {@link StreamOperator}
   */
  public static <M extends Message, OM extends Message> StreamOperator<M, OM> getStreamOperator(Function<M, Collection<OM>> transformFn) {
    return new StreamOperator<>(transformFn);
  }

  /**
   * The method only to be used internally in {@link MessageStream} to create {@link SinkOperator}
   *
   * @param sinkFn  the sink function
   * @param <M>  type of input {@link Message}
   * @return   the {@link SinkOperator}
   */
  public static <M extends Message> SinkOperator<M> getSinkOperator(MessageStream.VoidFunction3<M, MessageCollector, TaskCoordinator> sinkFn) {
    return new SinkOperator<>(sinkFn);
  }

  /**
   * The method only to be used internally in {@link MessageStream} to create {@link WindowOperator}
   *
   * @param windowFn  the {@link WindowFn} function
   * @param <M>  type of input {@link Message}
   * @param <WK>  type of window key
   * @param <WS>  type of {@link WindowState}
   * @param <WM>  type of output {@link WindowOutput}
   * @return  the {@link WindowOperator}
   */
  public static <M extends Message, WK, WS extends WindowState, WM extends WindowOutput<WK, ?>> WindowOperator<M, WK, WS, WM> getWindowOperator(
      WindowFn<M, WK, WS, WM> windowFn) {
    return new WindowOperator<>(windowFn, Operators.getOperatorId());
  }

  /**
   * The method only to be used internally in {@link MessageStream} to create {@link WindowOperator}
   *
   * @param joiner  the {@link WindowFn} function
   * @param joinOutput  the output {@link MessageStream}
   * @param <M>  type of input {@link Message}
   * @param <K>  type of join key
   * @param <JM>  the type of message in the {@link Message} from the other join stream
   * @param <RM>  the type of message in the {@link Message} from the join function
   * @return  the {@link PartialJoinOperator}
   */
  public static <M extends Message<K, ?>, K, JM extends Message<K, ?>, RM extends Message> PartialJoinOperator<M, K, JM, RM> getPartialJoinOperator(
      BiFunction<M, JM, RM> joiner, MessageStream<RM> joinOutput) {
    return new PartialJoinOperator<>(joiner, joinOutput, Operators.getOperatorId());
  }

  /**
   * The method only to be used internally in {@link MessageStream} to create {@link StreamOperator} as a merger function
   *
   * @param mergeOutput  the common output {@link MessageStream} from the merger
   * @param <M>  the type of input {@link Message}
   * @return  the {@link StreamOperator} for merge
   */
  public static <M extends Message> StreamOperator<M, M> getMergeOperator(MessageStream<M> mergeOutput) {
    return new StreamOperator<M, M>(t ->
      new ArrayList<M>() { {
          this.add(t);
        } },
      mergeOutput);
  }
}
