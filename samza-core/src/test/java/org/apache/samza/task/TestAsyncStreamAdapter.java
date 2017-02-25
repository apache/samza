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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


public class TestAsyncStreamAdapter {
  TestStreamTask task;
  AsyncStreamTaskAdapter taskAdaptor;
  Exception e;
  IncomingMessageEnvelope envelope;

  class TestCallbackListener implements TaskCallbackListener {
    boolean callbackComplete = false;
    boolean callbackFailure = false;

    @Override
    public void onComplete(TaskCallback callback) {
      callbackComplete = true;
    }

    @Override
    public void onFailure(TaskCallback callback, Throwable t) {
      callbackFailure = true;
    }
  }

  class TestStreamTask implements StreamTask, InitableTask, ClosableTask, WindowableTask {
    boolean inited = false;
    boolean closed = false;
    boolean processed = false;
    boolean windowed = false;

    @Override
    public void close() throws Exception {
      closed = true;
    }

    @Override
    public void init(Config config, TaskContext context) throws Exception {
      inited = true;
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
      processed = true;
      if (e != null) {
        throw e;
      }
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
      windowed = true;
    }
  }

  @Before
  public void setup() {
    task = new TestStreamTask();
    e = null;
    envelope = mock(IncomingMessageEnvelope.class);
  }

  @Test
  public void testAdapterWithoutThreadPool() throws Exception {
    taskAdaptor = new AsyncStreamTaskAdapter(task, null);
    TestCallbackListener listener = new TestCallbackListener();
    TaskCallback callback = new TaskCallbackImpl(listener, null, envelope, null, 0L, 0L);

    taskAdaptor.init(null, null);
    assertTrue(task.inited);

    taskAdaptor.processAsync(null, null, null, callback);
    assertTrue(task.processed);
    assertTrue(listener.callbackComplete);

    e = new Exception("dummy exception");
    taskAdaptor.processAsync(null, null, null, callback);
    assertTrue(listener.callbackFailure);

    taskAdaptor.window(null, null);
    assertTrue(task.windowed);

    taskAdaptor.close();
    assertTrue(task.closed);
  }

  @Test
  public void testAdapterWithThreadPool() throws Exception {
    TestCallbackListener listener1 = new TestCallbackListener();
    TaskCallback callback1 = new TaskCallbackImpl(listener1, null, envelope, null, 0L, 0L);

    TestCallbackListener listener2 = new TestCallbackListener();
    TaskCallback callback2 = new TaskCallbackImpl(listener2, null, envelope, null, 1L, 0L);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    taskAdaptor = new AsyncStreamTaskAdapter(task, executor);
    taskAdaptor.processAsync(null, null, null, callback1);
    taskAdaptor.processAsync(null, null, null, callback2);

    executor.awaitTermination(1, TimeUnit.SECONDS);
    assertTrue(listener1.callbackComplete);
    assertTrue(listener2.callbackComplete);

    e = new Exception("dummy exception");
    taskAdaptor.processAsync(null, null, null, callback1);
    taskAdaptor.processAsync(null, null, null, callback2);

    executor.awaitTermination(1, TimeUnit.SECONDS);
    assertTrue(listener1.callbackFailure);
    assertTrue(listener2.callbackFailure);
  }
}
