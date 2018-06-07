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

package org.apache.samza.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Shutdown related utils
 */
public class ShutdownUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ShutdownUtil.class);

  /**
   * A helper to facilitate shutting down a set of resources in parallel to enforce a bounded shutdown time.
   * The helper function instantiates an {@link ExecutorService} to execute the "shutdown function", and will
   * await the termination for given timeout. If shutdown remains unfinished in the end, the whole thread dump
   * will be printed to help debugging.
   *
   * The shutdown is performed with best-effort. Depending on the implementation of the shutdown function, resource
   * leak might be possible.
   *
   * @param shutdownFunction the shutdown function will be provided with an ExecutorService and is supposed to
   *                         submit all the shutdown tasks via the given executor
   * @param message message that will show in the thread name and the thread dump
   * @param timeoutMs timeout in ms
   */
  public static void boundedShutdown(Function<ExecutorService, Void> shutdownFunction, String message, long timeoutMs) {
    ExecutorService shutdownExecutorService = Executors.newCachedThreadPool(new ThreadFactory() {
      private final AtomicInteger counter = new AtomicInteger(0);
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName(message + "-" + counter.incrementAndGet());
        return thread;
      }
    });
    shutdownFunction.apply(shutdownExecutorService);
    shutdownExecutorService.shutdown();
    try {
      shutdownExecutorService.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.error("Shutdown was interrupted for " + message, e);
    }

    if (!shutdownExecutorService.isTerminated()) {
      LOG.error("Shutdown function for {} remains unfinished after timeout({}ms) or interruption", message, timeoutMs);
      Util.logThreadDump(message);
      shutdownExecutorService.shutdownNow();
    } else {
      LOG.info("Shutdown complete for {}", message);
    }
  }
}
