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

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


public class TestTaskCallbackImpl {

  TaskCallbackListener listener = null;
  AtomicInteger completeCount;
  AtomicInteger failureCount;
  TaskCallback callback = null;
  Throwable throwable = null;

  @Before
  public void setup() {
    completeCount = new AtomicInteger(0);
    failureCount = new AtomicInteger(0);
    throwable = null;

    listener = new TaskCallbackListener() {

      @Override
      public void onComplete(TaskCallback callback) {
        completeCount.incrementAndGet();
      }

      @Override
      public void onFailure(TaskCallback callback, Throwable t) {
        throwable = t;
        failureCount.incrementAndGet();
      }
    };

    callback = new TaskCallbackImpl(listener, null, mock(IncomingMessageEnvelope.class), null, 0L, 0L);
  }

  @Test
  public void testComplete() {
    callback.complete();
    assertEquals(1L, completeCount.get());
    assertEquals(0L, failureCount.get());
  }

  @Test
  public void testFailure() {
    callback.failure(new Exception("dummy exception"));
    assertEquals(0L, completeCount.get());
    assertEquals(1L, failureCount.get());
  }

  @Test
  public void testCallbackMultipleComplete() {
    callback.complete();
    assertEquals(1L, completeCount.get());

    callback.complete();
    assertEquals(1L, failureCount.get());
    assertTrue(throwable instanceof IllegalStateException);
  }

  @Test
  public void testCallbackFailureAfterComplete() {
    callback.complete();
    assertEquals(1L, completeCount.get());

    callback.failure(new Exception("dummy exception"));
    assertEquals(1L, failureCount.get());
    assertTrue(throwable instanceof IllegalStateException);
  }


  @Test
  public void testMultithreadedCallbacks() throws Exception {
    final CyclicBarrier barrier = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);

    for (int i = 0; i < 2; i++) {
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            barrier.await();
            callback.complete();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    }
    executor.awaitTermination(1, TimeUnit.SECONDS);
    assertEquals(1L, completeCount.get());
    assertEquals(1L, failureCount.get());
    assertTrue(throwable instanceof IllegalStateException);
  }
}
