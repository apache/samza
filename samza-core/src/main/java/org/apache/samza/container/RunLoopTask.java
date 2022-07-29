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

import java.util.Set;
import org.apache.samza.checkpoint.OffsetManager;
import org.apache.samza.scheduler.EpochTimeScheduler;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.ReadableCoordinator;
import org.apache.samza.task.TaskCallbackFactory;


/**
 * The interface required for a task's execution to be managed within {@link RunLoop}.
 *
 * Some notes on thread safety and exclusivity between methods:
 *
 * TODO SAMZA-2531: isAsyncCommitEnabled is either an incomplete feature or misnamed
 * RunLoop will ensure exclusivity between {@link #window}, {@link #commit}, {@link #scheduler}, and
 * {@link #endOfStream}.
 *
 * There is an exception for {@link #process}, which may execute concurrently with {@link #commit} IF the encapsulating
 * {@link RunLoop} has its isAsyncCommitEnabled set to true. In this case, the implementer of this interface should
 * take care to ensure that any objects shared between commit and process are thread safe.
 *
 * Be aware that {@link #commit}, {@link #window} and {@link #scheduler} can be run in their own thread pool outside
 * the main RunLoop thread (which executes {@link #process}) so may run concurrently between tasks. For example, one
 * task may be executing a commit while another is executing window. For this reason, implementers of this class must
 * ensure that objects shared between instances of RunLoopTask are thread safe.
 */
public interface RunLoopTask {

  /**
   * The {@link TaskName} associated with this RunLoopTask.
   *
   * @return taskName
   */
  TaskName taskName();

  /**
   * Process an incoming message envelope.
   *
   * @param envelope The envelope to be processed
   * @param coordinator Manages execution of tasks
   * @param callbackFactory Creates a callback to be used to indicate completion of or failure to process the
   *                        envelope. {@link TaskCallbackFactory#createCallback()} should be called before processing
   *                        begins.
   */
  void process(IncomingMessageEnvelope envelope, ReadableCoordinator coordinator, TaskCallbackFactory callbackFactory);

  /**
   * Performs a window for this task. If {@link #isWindowableTask()} is true, this method will be invoked periodically
   * by {@link RunLoop} according to its windowMs.
   *
   * This method can be used to perform aggregations within a task.
   *
   * @param coordinator Manages execution of tasks
   */
  void window(ReadableCoordinator coordinator);

  /**
   * Used in conjunction with {@link #epochTimeScheduler()} to execute scheduled callbacks. See documentation of
   * {@link EpochTimeScheduler} for more information.
   *
   * @param coordinator Manages execution of tasks.
   */
  void scheduler(ReadableCoordinator coordinator);

  /**
   * Performs a commit for this task. Operations for persisting checkpoint-related information for this task should
   * be done here.
   */
  void commit();

  /**
   * Called when all {@link SystemStreamPartition} processed by a task have reached end of stream. This is called only
   * once per task. {@link RunLoop} will issue a shutdown request to the coordinator immediately following the
   * invocation of this method.
   *
   * @param coordinator manages execution of tasks.
   */
  void endOfStream(ReadableCoordinator coordinator);

  /**
   * Called when all {@link SystemStreamPartition} processed by a task have drained. This is called only
   * once per task. {@link RunLoop} will issue a shutdown request to the coordinator immediately following the
   * invocation of this method.
   *
   * @param coordinator manages execution of tasks.
   */
  void drain(ReadableCoordinator coordinator);

  /**
   * Indicates whether {@link #window} should be invoked on this task. If true, {@link RunLoop}
   * will schedule window to execute periodically according to its windowMs.
   *
   * @return whether the task should perform window
   */
  boolean isWindowableTask();

  /**
   * Whether this task has intermediate streams. Intermediate streams may be used to facilitate task processing
   * before terminal output is produced. {@link RunLoop} uses this information to determine when the task has reached
   * end of stream.
   *
   * @return whether the task uses intermediate streams
   */
  Set<String> intermediateStreams();

  /**
   * The set of {@link SystemStreamPartition} this task consumes from.
   *
   * @return the set of SSPs
   */
  Set<SystemStreamPartition> systemStreamPartitions();

  /**
   * An {@link OffsetManager}, if any, to use to track offsets for each input SSP. Offsets will be updated after successful
   * completion of an envelope from an SSP.
   *
   * @return the offset manager, or null otherwise
   */
  OffsetManager offsetManager();

  /**
   * The metrics instance {@link RunLoop} will use to emit metrics related to execution of this task.
   *
   * @return metrics instance for this task
   */
  TaskInstanceMetrics metrics();

  /**
   * An {@link EpochTimeScheduler}, if any, used by the task to handle timer based callbacks.
   *
   * @return the scheduler, or null otherwise
   */
  EpochTimeScheduler epochTimeScheduler();
}