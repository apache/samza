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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.context.Context;
import org.apache.samza.operators.spec.BroadcastOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.system.ControlMessage;
import org.apache.samza.system.DrainMessage;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Collections;

class BroadcastOperatorImpl<M> extends OperatorImpl<M, Void> {

  private final BroadcastOperatorSpec<M> broadcastOpSpec;
  private final SystemStream systemStream;
  private final String taskName;
  private final String runId;

  BroadcastOperatorImpl(BroadcastOperatorSpec<M> broadcastOpSpec, SystemStream systemStream, Context context) {
    this.broadcastOpSpec = broadcastOpSpec;
    this.systemStream = systemStream;
    this.taskName = context.getTaskContext().getTaskModel().getTaskName().getTaskName();
    this.runId = new ApplicationConfig(context.getJobContext().getConfig()).getRunId();
  }

  @Override
  protected void handleInit(Context context) {
  }

  @Override
  protected CompletionStage<Collection<Void>> handleMessageAsync(M message, MessageCollector collector,
      TaskCoordinator coordinator) {
    collector.send(new OutgoingMessageEnvelope(systemStream, 0, null, message));
    return CompletableFuture.completedFuture(Collections.emptyList());
  }

  @Override
  protected void handleClose() {
  }

  @Override
  protected OperatorSpec<M, Void> getOperatorSpec() {
    return broadcastOpSpec;
  }

  @Override
  protected Collection<Void> handleEndOfStream(MessageCollector collector, TaskCoordinator coordinator) {
    sendControlMessage(new EndOfStreamMessage(taskName), collector);
    return Collections.emptyList();
  }

  @Override
  protected Collection<Void> handleDrain(MessageCollector collector, TaskCoordinator coordinator) {
    sendControlMessage(new DrainMessage(taskName, runId), collector);
    return Collections.emptyList();
  }

  @Override
  protected Collection<Void> handleWatermark(long watermark, MessageCollector collector, TaskCoordinator coordinator) {
    sendControlMessage(new WatermarkMessage(watermark, taskName), collector);
    return Collections.emptyList();
  }

  private void sendControlMessage(ControlMessage message, MessageCollector collector) {
    OutgoingMessageEnvelope envelopeOut = new OutgoingMessageEnvelope(systemStream, 0, null, message);
    collector.send(envelopeOut);
  }
}
