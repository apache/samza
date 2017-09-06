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

import java.util.Collection;
import java.util.Collections;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskContextImpl;
import org.apache.samza.system.ControlMessage;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


/**
 * An operator that sends incoming messages to an output {@link SystemStream}.
 */
class OutputOperatorImpl<M> extends OperatorImpl<M, Void> {

  private final OutputOperatorSpec<M> outputOpSpec;
  private final OutputStreamImpl<?, ?, M> outputStream;
  private final String taskName;
  private final ControlMessageSender controlMessageSender;

  OutputOperatorImpl(OutputOperatorSpec<M> outputOpSpec, Config config, TaskContext context) {
    this.outputOpSpec = outputOpSpec;
    this.outputStream = outputOpSpec.getOutputStream();
    this.taskName = context.getTaskName().getTaskName();

    StreamMetadataCache streamMetadataCache = ((TaskContextImpl) context).getStreamMetadataCache();
    this.controlMessageSender = new ControlMessageSender(streamMetadataCache);
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
  }

  @Override
  public Collection<Void> handleMessage(M message, MessageCollector collector,
      TaskCoordinator coordinator) {
    // TODO: SAMZA-1148 - need to find a way to directly pass in the serde class names
    SystemStream systemStream = new SystemStream(outputStream.getStreamSpec().getSystemName(),
        outputStream.getStreamSpec().getPhysicalName());
    Object key = outputStream.getKeyExtractor().apply(message);
    Object msg = outputStream.getMsgExtractor().apply(message);
    collector.send(new OutgoingMessageEnvelope(systemStream, key, msg));
    return Collections.emptyList();
  }

  @Override
  protected void handleClose() {
  }

  @Override
  protected OperatorSpec<M, Void> getOperatorSpec() {
    return outputOpSpec;
  }

  @Override
  protected void handleEndOfStream(MessageCollector collector, TaskCoordinator coordinator) {
    if (outputOpSpec.getOpCode() == OperatorSpec.OpCode.PARTITION_BY) {
      sendControlMessage(new EndOfStreamMessage(taskName), collector);
    }
  }

  @Override
  protected Long handleWatermark(long watermark, MessageCollector collector, TaskCoordinator coordinator) {
    if (outputOpSpec.getOpCode() == OperatorSpec.OpCode.PARTITION_BY) {
      sendControlMessage(new WatermarkMessage(watermark, taskName), collector);
    }
    return watermark;
  }

  private void sendControlMessage(ControlMessage message, MessageCollector collector) {
    SystemStream outputStream = outputOpSpec.getOutputStream().getStreamSpec().toSystemStream();
    controlMessageSender.send(message, outputStream, collector);
  }
}
