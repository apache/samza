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

import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.config.Config;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Collections;


/**
 * An operator that builds the input message from the incoming message.
 *
 * @param <K> the type of key in the incoming message
 * @param <V> the type of message in the incoming message
 * @param <M> the type of input message
 */
public final class InputOperatorImpl<K, V, M> extends OperatorImpl<Pair<K, V>, M> {

  private final InputOperatorSpec<K, V, M> inputOpSpec;

  InputOperatorImpl(InputOperatorSpec<K, V, M> inputOpSpec) throws IOException, ClassNotFoundException {
    this.inputOpSpec = inputOpSpec;
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
  }

  @Override
  public Collection<M> handleMessage(Pair<K, V> pair, MessageCollector collector, TaskCoordinator coordinator) {
    // TODO: SAMZA-1148 - Cast to appropriate input (key, msg) types based on the serde before applying the msgBuilder.
    M message = this.inputOpSpec.getMsgBuilder().apply(pair.getKey(), pair.getValue());
    return Collections.singletonList(message);
  }

  @Override
  protected void handleClose() {
  }

  protected OperatorSpec<Pair<K, V>, M> getOperatorSpec() {
    return this.inputOpSpec;
  }
}
