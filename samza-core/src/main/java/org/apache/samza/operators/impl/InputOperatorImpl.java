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
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;


/**
 * An operator that builds the input message from the incoming message.
 */
public final class InputOperatorImpl extends OperatorImpl<IncomingMessageEnvelope, Object> {

  private final InputOperatorSpec inputOpSpec;

  InputOperatorImpl(InputOperatorSpec inputOpSpec) {
    this.inputOpSpec = inputOpSpec;
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
  }

  @Override
  public Collection<Object> handleMessage(IncomingMessageEnvelope ime, MessageCollector collector, TaskCoordinator coordinator) {
    Object message;
    Optional<InputTransformer<?>> transformerOptional = inputOpSpec.getTransformer();
    if (transformerOptional.isPresent()) {
      message = transformerOptional.get().apply(ime);
    } else {
      message = this.inputOpSpec.isKeyed() ? KV.of(ime.getKey(), ime.getMessage()) : ime.getMessage();
    }
    return Collections.singletonList(message);
  }

  @Override
  protected void handleClose() {
  }

  protected OperatorSpec<IncomingMessageEnvelope, Object> getOperatorSpec() {
    return this.inputOpSpec;
  }
}
