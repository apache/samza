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
package org.apache.samza.operators.impl;

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

public class WindowOperatorImpl<M extends MessageEnvelope, K, WK, WV, WM extends WindowPane<WK, WV>> extends OperatorImpl<M, WM> {

  private final WindowInternal<M, K, WV> window;

  public WindowOperatorImpl(WindowOperatorSpec spec) {
    window = spec.getWindow();
  }

  @Override
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {

  }
}
