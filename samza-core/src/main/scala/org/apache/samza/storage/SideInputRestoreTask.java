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

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.container.RunLoopTask;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.ReadableCoordinator;
import org.apache.samza.task.TaskCallbackFactory;
import scala.collection.JavaConversions;


class SideInputRestoreTask extends RunLoopTask {

  private final TaskName taskName;
  private final Set<SystemStreamPartition> taskSSPs;
  private final TaskSideInputHandler taskSideInputHandler;
  private final TaskSideInputStorageManager taskSideInputStorageManager;
  private final TaskInstanceMetrics metrics;

  public SideInputRestoreTask(
      TaskName taskName,
      Set<SystemStreamPartition> taskSSPs,
      TaskSideInputHandler taskSideInputHandler,
      TaskSideInputStorageManager taskSideInputStorageManager,
      TaskInstanceMetrics metrics) {
    this.taskName = taskName;
    this.taskSSPs = taskSSPs;
    this.taskSideInputHandler = taskSideInputHandler;
    this.taskSideInputStorageManager = taskSideInputStorageManager;
    this.metrics = metrics;
  }

  @Override
  public void process(
      IncomingMessageEnvelope envelope, ReadableCoordinator coordinator, TaskCallbackFactory callbackFactory) {
    this.taskSideInputHandler.process(envelope, callbackFactory);
  }

  @Override
  public void commit() {
    CheckpointId checkpointId = CheckpointId.create();
    Map<SystemStreamPartition, String> lastProcessedOffsets = this.taskSSPs.stream()
        .collect(Collectors.toMap(Function.identity(), this.taskSideInputHandler::getLastProcessedOffset));

    this.taskSideInputStorageManager.flush();
    Map<String, Path> checkpointPaths = this.taskSideInputStorageManager.checkpoint(checkpointId);
    this.taskSideInputStorageManager.writeOffsetFiles(lastProcessedOffsets, checkpointPaths);
    this.taskSideInputStorageManager.removeOldCheckpoints(checkpointId.toString());
  }

  @Override
  public TaskInstanceMetrics metrics() {
    return this.metrics;
  }

  @Override
  public scala.collection.immutable.Set<SystemStreamPartition> systemStreamPartitions() {
    return JavaConversions.asScalaSet(taskSSPs).toSet();
  }

  @Override
  public TaskName taskName() {
    return taskName;
  }
}
