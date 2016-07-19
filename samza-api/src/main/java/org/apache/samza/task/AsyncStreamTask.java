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

package org.apache.samza.task;

import org.apache.samza.system.IncomingMessageEnvelope;

/**
 * An AsyncStreamTask is the basic class to support multithreading execution in Samza container. It’s provided for better
 * parallelism and resource utilization. This class allows task to make asynchronous calls and fire callbacks upon completion.
 * Similar to {@link StreamTask}, an AsyncStreamTask may be augmented by implementing other interfaces, such as
 * {@link InitableTask}, {@link WindowableTask}, or {@link ClosableTask}. The following invariants hold with these mix-ins:
 *
 * InitableTask.init - always the first method invoked on an AsyncStreamTask. It happens-before every subsequent
 * invocation on AsyncStreamTask (for happens-before semantics, see https://docs.oracle.com/javase/tutorial/essential/concurrency/memconsist.html).
 *
 * CloseableTask.close - always the last method invoked on an AsyncStreamTask and all other AsyncStreamTask are guaranteed
 * to happen-before it.
 *
 * AsyncStreamTask.processAsync - can run in either a serialized or parallel mode. In the serialized mode (task.process.max.inflight.messages=1),
 * each invocation of processAsync is guaranteed to happen-before the next. In a parallel execution mode (task.process.max.inflight.messages&gt;1),
 * there is no such happens-before constraint and the AsyncStreamTask is required to coordinate any shared state.
 *
 * WindowableTask.window - in either above mode, it is called when no invocations to processAsync are pending and no new
 * processAsync invocations can be scheduled until it completes. Therefore, a guarantee that all previous processAsync invocations
 * happen before an invocation of WindowableTask.window. An invocation to WindowableTask.window is guaranteed to happen-before
 * any subsequent processAsync invocations. The Samza engine is responsible for ensuring that window is invoked in a timely manner.
 *
 * Similar to WindowableTask.window, commits are guaranteed to happen only when there are no pending processAsync or WindowableTask.window
 * invocations. All preceding invocations happen-before commit and commit happens-before all subsequent invocations.
 */
public interface AsyncStreamTask {
  /**
   * Called once for each message that this AsyncStreamTask receives.
   * @param envelope Contains the received deserialized message and key, and also information regarding the stream and
   * partition of which the message was received from.
   * @param collector Contains the means of sending message envelopes to the output stream. The collector must only
   * be used during the current call to the process method; you should not reuse the collector between invocations
   * of this method.
   * @param coordinator Manages execution of tasks.
   * @param callback Triggers the completion of the process.
   */
  void processAsync(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator, TaskCallback callback);
}