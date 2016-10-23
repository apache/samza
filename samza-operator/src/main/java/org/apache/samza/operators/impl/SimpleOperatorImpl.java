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

import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.internal.Operators.StreamOperator;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.function.Function;


/**
 * Base class for all implementation of operators
 *
 * @param <M>  type of message in the input stream
 * @param <RM>  type of message in the output stream
 */
public class SimpleOperatorImpl<M extends Message, RM extends Message> extends OperatorImpl<M, RM> {

  private final Function<M, Collection<RM>> transformFn;

  SimpleOperatorImpl(StreamOperator<M, RM> op) {
    super();
    this.transformFn = op.getFunction();
  }

  @Override protected void onNext(M imsg, MessageCollector collector, TaskCoordinator coordinator) {
    // actually calling the transform function and then for each output, call nextProcessors()
    this.transformFn.apply(imsg).forEach(r -> this.nextProcessors(r, collector, coordinator));
  }
}
