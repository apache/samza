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
package org.apache.samza.storage;

import java.util.Collections;
import java.util.Set;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.container.RunLoopTask;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.scheduler.EpochTimeScheduler;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.ReadableCoordinator;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskCallbackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class encapsulates the processing logic for side input streams. It is executed by {@link org.apache.samza.container.RunLoop}
 */
public class SideInputTask implements RunLoopTask {
  private static final Logger LOG = LoggerFactory.getLogger(SideInputTask.class);

  private final TaskName taskName;
  private final Set<SystemStreamPartition> taskSSPs;
  private final TaskSideInputHandler taskSideInputHandler;
  private final TaskInstanceMetrics metrics;

  public SideInputTask(
      TaskName taskName,
      Set<SystemStreamPartition> taskSSPs,
      TaskSideInputHandler taskSideInputHandler,
      TaskInstanceMetrics metrics) {
    this.taskName = taskName;
    this.taskSSPs = taskSSPs;
    this.taskSideInputHandler = taskSideInputHandler;
    this.metrics = metrics;
  }

  @Override
  public TaskName taskName() {
    return this.taskName;
  }

  @Override
  synchronized public void process(IncomingMessageEnvelope envelope, ReadableCoordinator coordinator,
      TaskCallbackFactory callbackFactory) {
    TaskCallback callback = callbackFactory.createCallback();
    this.metrics.processes().inc();
    try {
      this.taskSideInputHandler.process(envelope);
      this.metrics.messagesActuallyProcessed().inc();
      callback.complete();
    } catch (Exception e) {
      callback.failure(e);
    }
  }

  @Override
  public void window(ReadableCoordinator coordinator) {
    throw new UnsupportedOperationException("Windowing is not applicable for side input tasks.");
  }

  @Override
  public void scheduler(ReadableCoordinator coordinator) {
    throw new UnsupportedOperationException("Scheduling is not applicable for side input tasks.");
  }

  @Override
  synchronized public void commit() {
    this.taskSideInputHandler.flush();
    this.metrics.commits().inc();
  }

  @Override
  public void endOfStream(ReadableCoordinator coordinator) {
    LOG.info("Task {} has reached end of stream", this.taskName);
  }

  @Override
  public void drain(ReadableCoordinator coordinator) {
    LOG.info("Task {} has drained", this.taskName);
  }

  @Override
  public boolean isWindowableTask() {
    return false;
  }

  @Override
  public Set<String> intermediateStreams() {
    return Collections.emptySet();
  }

  @Override
  public Set<SystemStreamPartition> systemStreamPartitions() {
    return this.taskSSPs;
  }

  @Override
  public OffsetManager offsetManager() {
    return null;
  }

  @Override
  public TaskInstanceMetrics metrics() {
    return this.metrics;
  }

  @Override
  public EpochTimeScheduler epochTimeScheduler() {
    return null;
  }
}
