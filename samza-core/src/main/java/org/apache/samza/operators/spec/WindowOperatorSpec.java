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
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.task.TaskContext;


/**
 * Default window operator spec object
 *
 * @param <M>  the type of input message to the window
 * @param <WK>  the type of key of the window
 * @param <WV>  the type of aggregated value in the window output {@link WindowPane}
 */
public class WindowOperatorSpec<M, WK, WV> implements OperatorSpec<WindowPane<WK, WV>> {

  private final WindowInternal<M, WK, WV> window;

  private final MessageStreamImpl<WindowPane<WK, WV>> outputStream;

  private final int opId;


  /**
   * Constructor for {@link WindowOperatorSpec}.
   *
   * @param window  the window function
   * @param outputStream  the output {@link MessageStreamImpl} from this {@link WindowOperatorSpec}
   * @param opId  auto-generated unique ID of this operator
   */
  WindowOperatorSpec(WindowInternal<M, WK, WV> window, MessageStreamImpl<WindowPane<WK, WV>> outputStream, int opId) {
    this.outputStream = outputStream;
    this.window = window;
    this.opId = opId;
  }

  @Override
  public void init(Config config, TaskContext context) {
    if (window.getFoldFunction() != null) {
      window.getFoldFunction().init(config, context);
    }
  }

  @Override
  public MessageStreamImpl<WindowPane<WK, WV>> getNextStream() {
    return this.outputStream;
  }

  public WindowInternal<M, WK, WV> getWindow() {
    return window;
  }

  public OpCode getOpCode() {
    return OpCode.WINDOW;
  }

  public int getOpId() {
    return this.opId;
  }
}
