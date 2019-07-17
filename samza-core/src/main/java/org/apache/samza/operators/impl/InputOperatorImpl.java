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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.system.descriptors.InputTransformer;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Collections;


/**
 * An operator that builds the input message from the incoming message.
 */
public final class InputOperatorImpl extends OperatorImpl<IncomingMessageEnvelope, Object> {

  private final InputOperatorSpec inputOpSpec;

  InputOperatorImpl(InputOperatorSpec inputOpSpec) {
    this.inputOpSpec = inputOpSpec;
  }

  @Override
  protected void handleInit(Context context) {
  }

  @Override
  protected CompletionStage<Collection<Object>> handleMessageAsync(IncomingMessageEnvelope message,
      MessageCollector collector, TaskCoordinator coordinator) {
    Object result;
    InputTransformer transformer = inputOpSpec.getTransformer();
    if (transformer != null) {
      result = transformer.apply(message);
    } else {
      result = this.inputOpSpec.isKeyed() ? KV.of(message.getKey(), message.getMessage()) : message.getMessage();
    }

    Collection<Object> output = Optional.ofNullable(result)
        .map(Collections::singletonList)
        .orElse(Collections.emptyList());

    return CompletableFuture.completedFuture(output);
  }

  @Override
  protected void handleClose() {
  }

  protected OperatorSpec<IncomingMessageEnvelope, Object> getOperatorSpec() {
    return this.inputOpSpec;
  }
}
