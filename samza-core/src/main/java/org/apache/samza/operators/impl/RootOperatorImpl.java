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

import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Collections;


/**
 * A no-op operator implementation that forwards incoming messages to all of its subscribers.
 * @param <M>  type of incoming messages
 */
public final class RootOperatorImpl<M> extends OperatorImpl<M, M> {

  @Override
  protected void handleInit(Config config, TaskContext context) {
  }

  @Override
  public Collection<M> handleMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    return Collections.singletonList(message);
  }

  // TODO: SAMZA-1221 - Change to InputOperatorSpec that also builds the message
  @Override
  protected OperatorSpec<M> getOperatorSpec() {
    return new OperatorSpec<M>() {
      @Override
      public MessageStreamImpl<M> getNextStream() {
        return null;
      }

      @Override
      public OpCode getOpCode() {
        return OpCode.INPUT;
      }

      @Override
      public int getOpId() {
        return -1;
      }

      @Override
      public String getSourceLocation() {
        return "";
      }
    };
  }
}
