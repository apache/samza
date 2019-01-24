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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.samza.context.Context;
import org.apache.samza.system.IncomingMessageEnvelope;


/**
 * AsyncStreamTaskAdapter allows a StreamTask to be executed in parallel.The class
 * uses the build-in thread pool to invoke StreamTask.process and triggers
 * the callbacks once it's done. If the thread pool is null, it follows the legacy
 * synchronous model to execute the tasks on the run loop thread.
 */
public class AsyncStreamTaskAdapter implements AsyncStreamTask, InitableTask, WindowableTask, ClosableTask, EndOfStreamListenerTask {
  private final StreamTask wrappedTask;
  private final ExecutorService executor;
  private final long shutdownMs;

  public AsyncStreamTaskAdapter(StreamTask task, ExecutorService executor, long shutdownMs) {
    this.wrappedTask = task;
    this.executor = executor;
    this.shutdownMs = shutdownMs;
  }

  @Override
  public void init(Context context) throws Exception {
    if (wrappedTask instanceof InitableTask) {
      ((InitableTask) wrappedTask).init(context);
    }
  }

  @Override
  public void processAsync(final IncomingMessageEnvelope envelope,
      final MessageCollector collector,
      final TaskCoordinator coordinator,
      final TaskCallback callback) {
    if (executor != null) {
      executor.submit(() -> process(envelope, collector, coordinator, callback));
    } else {
      // legacy mode: running all tasks in the runloop thread
      process(envelope, collector, coordinator, callback);
    }
  }

  private void process(IncomingMessageEnvelope envelope,
      MessageCollector collector,
      TaskCoordinator coordinator,
      TaskCallback callback) {
    try {
      wrappedTask.process(envelope, collector, coordinator);
      callback.complete();
    } catch (Throwable t) {
      callback.failure(t);
    }
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if (wrappedTask instanceof WindowableTask) {
      ((WindowableTask) wrappedTask).window(collector, coordinator);
    }
  }

  @Override
  public void close() throws Exception {
    if (wrappedTask instanceof ClosableTask) {
      ((ClosableTask) wrappedTask).close();
    }

    if (executor != null) {
      executor.shutdown();
      if (executor.awaitTermination(shutdownMs, TimeUnit.MILLISECONDS)) {
        executor.shutdownNow();
      }
    }
  }

  @Override
  public void onEndOfStream(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if (wrappedTask instanceof EndOfStreamListenerTask) {
      ((EndOfStreamListenerTask) wrappedTask).onEndOfStream(collector, coordinator);
    }
  }
}
