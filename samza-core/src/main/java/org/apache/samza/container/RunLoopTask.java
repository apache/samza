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
package org.apache.samza.container;

import java.util.Collections;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.scheduler.EpochTimeScheduler;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.ReadableCoordinator;
import org.apache.samza.task.TaskCallbackFactory;
import scala.collection.JavaConversions;


public interface RunLoopTask {

  TaskName taskName();

  default boolean isWindowableTask() {
    return false;
  }

  default boolean isAsyncTask() {
    return false;
  }

  default EpochTimeScheduler epochTimeScheduler() {
    return null;
  }

  default scala.collection.immutable.Set<String> intermediateStreams() {
    return JavaConversions.asScalaSet(Collections.emptySet()).toSet();
  }

  scala.collection.immutable.Set<SystemStreamPartition> systemStreamPartitions();

  TaskInstanceMetrics metrics();

  void process(IncomingMessageEnvelope envelope, ReadableCoordinator coordinator, TaskCallbackFactory callbackFactory);

  void endOfStream(ReadableCoordinator coordinator);

  void window(ReadableCoordinator coordinator);

  void scheduler(ReadableCoordinator coordinator);

  void commit();

  default OffsetManager offsetManager() {
    return null;
  }
}