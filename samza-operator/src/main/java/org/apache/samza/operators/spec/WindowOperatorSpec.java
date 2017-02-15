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
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;

public class WindowOperatorSpec<M extends MessageEnvelope, K, WK, WV, WM extends WindowPane<WK, WV>> implements OperatorSpec<WM> {

  private final WindowInternal window;

  private final MessageStreamImpl<WM> outputStream;

  private final String operatorId;


  public WindowOperatorSpec(WindowInternal window, String operatorId) {
    this.window = window;
    this.outputStream = new MessageStreamImpl<>();
    this.operatorId = operatorId;
  }

  @Override
  public MessageStream<WM> getOutputStream() {
    return this.outputStream;
  }

  public WindowInternal getWindow() {
    return window;
  }

  public String getOperatorId() {
    return operatorId;
  }
}
