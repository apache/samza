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

import org.apache.samza.operators.api.internal.Operators.SinkOperator;
import org.apache.samza.operators.api.MessageStream;
import org.apache.samza.operators.api.data.Message;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


/**
 * Implementation for {@link SinkOperator}
 */
public class SinkOperatorImpl<M extends Message> extends OperatorImpl<M, Message> {
  private final MessageStream.VoidFunction3<M, MessageCollector, TaskCoordinator> sinkFunc;

  SinkOperatorImpl(SinkOperator<M> sinkOp) {
    this.sinkFunc = sinkOp.getFunction();
  }

  @Override protected void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    this.sinkFunc.apply(message, collector, coordinator);
  }
}
