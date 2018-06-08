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


import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;

/**
 * The spec for an operator that outputs a {@link org.apache.samza.operators.MessageStream} to a
 * {@link org.apache.samza.system.SystemStream}.
 * <p>
 * This is a terminal operator and does not allow further operator chaining.
 *
 * @param <M>  the type of input message
 */
public class OutputOperatorSpec<M> extends OperatorSpec<M, Void> {

  private final OutputStreamImpl<M> outputStream;

  /**
   * Constructs an {@link OutputOperatorSpec} to send messages to the provided {@code outStream}
   *
   * @param outputStream  the {@link OutputStreamImpl} to send messages to
   * @param opId  the unique ID of this {@link SinkOperatorSpec} in the graph
   */
  OutputOperatorSpec(OutputStreamImpl<M> outputStream, String opId) {
    super(OpCode.SEND_TO, opId);
    this.outputStream = outputStream;
  }

  /**
   * The {@link OutputStreamImpl} that this operator is sending its output to.
   * @return the {@link OutputStreamImpl} for this operator if any, else null.
   */
  public OutputStreamImpl<M> getOutputStream() {
    return this.outputStream;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return null;
  }

  @Override
  public TimerFunction getTimerFn() {
    return null;
  }
}
