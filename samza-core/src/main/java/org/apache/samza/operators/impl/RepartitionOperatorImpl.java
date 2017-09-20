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

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.spec.RepartitionOperatorSpec;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Collections;


/**
 * An operator that sends sends messages to an output {@link SystemStream} for repartitioning them.
 */
class RepartitionOperatorImpl<M, K, V> extends OperatorImpl<M, Void> {

  private final RepartitionOperatorSpec<M, K, V> repartitionOpSpec;
  private final SystemStream systemStream;
  private final MapFunction<? super M, ? extends K> keyFunction;
  private final MapFunction<? super M, ? extends V> valueFunction;

  RepartitionOperatorImpl(RepartitionOperatorSpec<M, K, V> repartitionOpSpec, Config config, TaskContext context) {
    this.repartitionOpSpec = repartitionOpSpec;
    OutputStreamImpl<KV<K, V>> outputStream = repartitionOpSpec.getOutputStream();
    if (!outputStream.isKeyedOutput()) {
      throw new SamzaException("Output stream for repartitioning must be a keyed stream.");
    }
    this.systemStream = new SystemStream(
        outputStream.getStreamSpec().getSystemName(),
        outputStream.getStreamSpec().getPhysicalName());
    this.keyFunction = repartitionOpSpec.getKeyFunction();
    this.valueFunction = repartitionOpSpec.getValueFunction();
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
  }

  @Override
  public Collection<Void> handleMessage(M message, MessageCollector collector,
      TaskCoordinator coordinator) {
    K key = keyFunction.apply(message);
    V value = valueFunction.apply(message);
    collector.send(new OutgoingMessageEnvelope(systemStream, null, key, value));
    return Collections.emptyList();
  }

  @Override
  protected void handleClose() {
  }

  @Override
  protected OperatorSpec<M, Void> getOperatorSpec() {
    return repartitionOpSpec;
  }
}
