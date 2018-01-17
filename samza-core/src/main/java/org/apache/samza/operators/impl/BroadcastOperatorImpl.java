package org.apache.samza.operators.impl;

import org.apache.samza.config.Config;
import org.apache.samza.operators.spec.BroadcastOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.system.ControlMessage;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by xiliu on 1/16/18.
 */
class BroadcastOperatorImpl<M> extends OperatorImpl<M, Void> {

  private final BroadcastOperatorSpec<M> broadcastOpSpec;
  private final SystemStream systemStream;
  private final String taskName;

  BroadcastOperatorImpl(BroadcastOperatorSpec<M> broadcastOpSpec, TaskContext context) {
    this.broadcastOpSpec = broadcastOpSpec;
    this.systemStream = broadcastOpSpec.getOutputStream().getStreamSpec().toSystemStream();
    this.taskName = context.getTaskName().getTaskName();
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
  }

  @Override
  protected Collection<Void> handleMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    collector.send(new OutgoingMessageEnvelope(systemStream, 0, null, message));
    return Collections.emptyList();
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
  protected Collection<Void> handleWatermark(long watermark, MessageCollector collector, TaskCoordinator coordinator) {
    sendControlMessage(new WatermarkMessage(watermark, taskName), collector);
    return Collections.emptyList();
  }

  private void sendControlMessage(ControlMessage message, MessageCollector collector) {
    OutgoingMessageEnvelope envelopeOut = new OutgoingMessageEnvelope(systemStream, 0, null, message);
    collector.send(envelopeOut);
  }
}
