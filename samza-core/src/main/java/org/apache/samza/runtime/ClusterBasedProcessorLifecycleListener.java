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

package org.apache.samza.runtime;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.SamzaContainerListener;
import org.apache.samza.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClusterBasedProcessorLifecycleListener implements SamzaContainerListener {
  private static final Logger log = LoggerFactory.getLogger(ClusterBasedProcessorLifecycleListener.class);
  private final Runnable onSignal;
  private Thread shutdownHookThread = null;
  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private TaskConfig taskConfig;
  private ProcessorLifecycleListener processorLifecycleListener;
  private volatile Throwable containerException = null;

  ClusterBasedProcessorLifecycleListener(Config config, ProcessorLifecycleListener processorLifecycleListener,
      Runnable onSignal) {
    this.taskConfig = new TaskConfig(config);
    this.processorLifecycleListener = processorLifecycleListener;
    this.onSignal = onSignal;
  }

  @Override
  public void beforeStart() {
    log.info("Before starting the container.");
    addShutdownHook();
    processorLifecycleListener.beforeStart();
  }

  @Override
  public void afterStart() {
    log.info("Container Started");
    processorLifecycleListener.afterStart();
  }

  @Override
  public void afterStop() {
    log.info("Container Stopped");
    processorLifecycleListener.afterStop();
    removeShutdownHook();
    shutdownLatch.countDown();
  }

  @Override
  public void afterFailure(Throwable t) {
    log.info("Container Failed");
    containerException = t;
    processorLifecycleListener.afterFailure(t);
    removeShutdownHook();
    shutdownLatch.countDown();
  }

  Throwable getContainerException() {
    return containerException;
  }

  private void removeShutdownHook() {
    try {
      if (shutdownHookThread != null) {
        removeJVMShutdownHook();
      }
    } catch (IllegalStateException e) {
      // Thrown when then JVM is already shutting down, so safe to ignore.
    }
  }

  private void addShutdownHook() {
    shutdownHookThread = new Thread("Samza Container Shutdown Hook Thread") {
      @Override
      public void run() {
        long shutdownMs = taskConfig.getShutdownMs();
        log.info("Attempting to shutdown container from inside shutdownHook, will wait up to {} ms.", shutdownMs);
        try {
          onSignal.run();
          boolean hasShutdown = shutdownLatch.await(shutdownMs, TimeUnit.MILLISECONDS);
          if (hasShutdown) {
            log.info("Shutdown complete");
          } else {
            log.error("Did not shut down within {} ms, exiting.", shutdownMs);
            ThreadUtil.logThreadDump("Thread dump from Samza Container Shutdown Hook.");
          }
        } catch (InterruptedException e) {
          log.error("Shutdown hook inturrupted while waiting on runLoop to shutdown");
          ThreadUtil.logThreadDump("Thread dump from Samza Container Shutdown Hook.");
        }
      }
    };
    addJVMShutdownHook();
  }

  @VisibleForTesting
  Thread getShutdownHookThread() {
    return this.shutdownHookThread;
  }

  @VisibleForTesting
  void addJVMShutdownHook() {
    Runtime.getRuntime().addShutdownHook(shutdownHookThread);
    log.info("Added Samza container shutdown hook");
  }

  @VisibleForTesting
  void removeJVMShutdownHook() {
    Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
    log.info("Removed Samza container shutdown hook");
  }
}
