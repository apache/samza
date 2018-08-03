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
import org.apache.samza.container.TaskContextImpl;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.PartitionByOperatorSpec;
import org.apache.samza.system.ControlMessage;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Collections;


/**
 * An operator that sends sends messages to an output {@link SystemStream} for repartitioning them.
 */
class PartitionByOperatorImpl<M, K, V> extends OperatorImpl<M, Void> {

  private final PartitionByOperatorSpec<M, K, V> partitionByOpSpec;
  private final SystemStream systemStream;
  private final MapFunction<? super M, ? extends K> keyFunction;
  private final MapFunction<? super M, ? extends V> valueFunction;
  private final String taskName;
  private final ControlMessageSender controlMessageSender;

  PartitionByOperatorImpl(PartitionByOperatorSpec<M, K, V> partitionByOpSpec,
      SystemStream systemStream, TaskContext context) {
    this.partitionByOpSpec = partitionByOpSpec;
    this.systemStream = systemStream;
    this.keyFunction = partitionByOpSpec.getKeyFunction();
    this.valueFunction = partitionByOpSpec.getValueFunction();
    this.taskName = context.getTaskName().getTaskName();
    StreamMetadataCache streamMetadataCache = ((TaskContextImpl) context).getStreamMetadataCache();
    this.controlMessageSender = new ControlMessageSender(streamMetadataCache);
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
    this.keyFunction.init(config, context);
    this.valueFunction.init(config, context);
  }

  @Override
  public Collection<Void> handleMessage(M message, MessageCollector collector,
      TaskCoordinator coordinator) {
    K key = keyFunction.apply(message);
    V value = valueFunction.apply(message);
    Long partitionKey = key == null ? 0L : null;
    collector.send(new OutgoingMessageEnvelope(systemStream, partitionKey, key, value));
    return Collections.emptyList();
  }

  @Override
  protected void handleClose() {
    this.keyFunction.close();
    this.valueFunction.close();
  }

  @Override
  protected OperatorSpec<M, Void> getOperatorSpec() {
    return partitionByOpSpec;
  }

  @Override
  protected Collection<Void> handleEndOfStream(MessageCollector collector, TaskCoordinator coordinator) {
    sendControlMessage(new EndOfStreamMessage(taskName), collector);
    return Collections.emptyList();
  }

  @Override
  protected Collection<Void> handleWatermark(long watermark, MessageCollector collector, TaskCoordinator coordinator) {
    sendControlMessage(new WatermarkMessage(watermark, taskName), collector);
    return Collections.emptyList();
  }

  private void sendControlMessage(ControlMessage message, MessageCollector collector) {
    controlMessageSender.send(message, systemStream, collector);
  }
}
