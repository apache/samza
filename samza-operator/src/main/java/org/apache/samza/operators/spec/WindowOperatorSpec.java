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

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.windows.Trigger;
import org.apache.samza.operators.windows.WindowFn;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.storage.kv.Entry;

import java.util.function.BiFunction;


/**
 * Defines a window operator that takes one {@link MessageStreamImpl} as an input, accumulates the window state,
 * and generates an output {@link MessageStreamImpl} with output type {@code WM} which extends {@link WindowOutput}
 *
 * @param <M>  the type of input {@link MessageEnvelope}
 * @param <WK>  the type of key in the output {@link MessageEnvelope} from the {@link WindowOperatorSpec} function
 * @param <WS>  the type of window state in the {@link WindowOperatorSpec} function
 * @param <WM>  the type of window output {@link MessageEnvelope}
 */
public class WindowOperatorSpec<M extends MessageEnvelope, WK, WS extends WindowState, WM extends WindowOutput<WK, ?>> implements
    OperatorSpec<WM> {

  /**
   * The output {@link MessageStream}.
   */
  private final MessageStreamImpl<WM> outputStream;

  /**
   * The window transformation function that takes {@link MessageEnvelope}s from one input stream, aggregates with the window
   * state(s) from the window state store, and generate output {@link MessageEnvelope}s for the output stream.
   */
  private final BiFunction<M, Entry<WK, WS>, WM> transformFn;

  /**
   * The state store functions for the {@link WindowOperatorSpec}.
   */
  private final StoreFunctions<M, WK, WS> storeFns;

  /**
   * The window trigger.
   */
  private final Trigger<M, WS> trigger;

  /**
   * The unique ID of this operator.
   */
  private final String operatorId;

  /**
   * Constructor for {@link WindowOperatorSpec}.
   *
   * @param windowFn  the window function
   * @param operatorId  auto-generated unique ID of this operator
   */
  WindowOperatorSpec(WindowFn<M, WK, WS, WM> windowFn, String operatorId) {
    this.outputStream = new MessageStreamImpl<>();
    this.transformFn = windowFn.getTransformFn();
    this.storeFns = windowFn.getStoreFns();
    this.trigger = windowFn.getTrigger();
    this.operatorId = operatorId;
  }

  @Override
  public String toString() {
    return this.operatorId;
  }

  @Override
  public MessageStreamImpl<WM> getOutputStream() {
    return this.outputStream;
  }

  public StoreFunctions<M, WK, WS> getStoreFns() {
    return this.storeFns;
  }

  public BiFunction<M, Entry<WK, WS>, WM> getTransformFn() {
    return this.transformFn;
  }

  public Trigger<M, WS> getTrigger() {
    return this.trigger;
  }

  /**
   * Method to generate the window operator's state store name
   * TODO HIGH pmaheshw: should this be here?
   *
   * @param inputStream the input {@link MessageStreamImpl} to this state store
   * @return   the persistent store name of the window operator
   */
  public String getStoreName(MessageStream<M> inputStream) {
    //TODO: need to get the persistent name of ds and the operator in a serialized form
    return String.format("input-%s-wndop-%s", inputStream.toString(), this.toString());
  }
}
