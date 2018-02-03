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
import org.apache.samza.operators.OpContext;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Collections;


/**
 * An operator that builds the input message from the incoming message.
 *
 * @param <K> the type of key in the incoming message
 * @param <V> the type of message in the incoming message
 */
public final class InputOperatorImpl<K, V> extends OperatorImpl<KV<K, V>, Object> { // Object == KV<K,V> | V

  private final InputOperatorSpec<K, V> inputOpSpec;

  InputOperatorImpl(InputOperatorSpec<K, V> inputOpSpec) {
    this.inputOpSpec = inputOpSpec;
  }

  @Override
  protected void handleInit(Config config, OpContext opContext) {
  }

  @Override
  public Collection<Object> handleMessage(KV<K, V> pair, MessageCollector collector, TaskCoordinator coordinator) {
    Object message = this.inputOpSpec.isKeyed() ? pair : pair.getValue();
    return Collections.singletonList(message);
  }

  @Override
  protected void handleClose() {
  }

  protected OperatorSpec<KV<K, V>, Object> getOperatorSpec() {
    return this.inputOpSpec;
  }
}
