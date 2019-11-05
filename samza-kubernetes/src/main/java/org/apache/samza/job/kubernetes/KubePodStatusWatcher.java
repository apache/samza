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

package org.apache.samza.job.kubernetes;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A monitor for the running Kubernetes pod of a Samza application
 */

// TODO: SAMZA-2369: Add a logging thread which is similar to LoggingPodStatusWatcher in Spark
public class KubePodStatusWatcher implements Watcher<Pod> {
  private static final Logger LOG = LoggerFactory.getLogger(KubeJob.class);
  private Optional<Pod> pod = Optional.empty();
  private String phase = "unknown";
  private String appId;
  private CountDownLatch podRunningLatch = new CountDownLatch(1);
  private CountDownLatch podPendingLatch = new CountDownLatch(1);
  private CountDownLatch podSucceededLatch = new CountDownLatch(1);
  private CountDownLatch podFailedLatch = new CountDownLatch(1);
  private CountDownLatch podCompletedLatch = new CountDownLatch(1);

  public KubePodStatusWatcher(String appId) {
    this.appId = appId;
  }

  @Override
  public void eventReceived(Action action, Pod pod) {
    this.pod = Optional.of(pod);
    switch (action) {
      case DELETED:
      case ERROR :
        closeAllWatch();
        break;
      default:
        if (isFailed()) {
          closeWatchWhenFailed();
        } else if(isSucceeded()) {
          closeWatchWhenSucceed();
        } else if (isRunning()) {
          closeWatchWhenRunning();
        } else if (isPending()) {
          closeWatchWhenPending();
        }
    }
  }

  @Override
  public void onClose(KubernetesClientException e) {
    LOG.info("Stopping watching application {} with last-observed phase {}", appId, phase);
    closeAllWatch();
  }

  public void waitForCompleted(long timeout, TimeUnit unit) {
    try {
      podCompletedLatch.await(timeout, unit);
    } catch (InterruptedException e) {
      LOG.error("waitForCompleted() was interrupted by exception: ", e);
    }
  }

  public void waitForSucceeded(long timeout, TimeUnit unit) {
    try {
      podSucceededLatch.await(timeout, unit);
    } catch (InterruptedException e) {
      LOG.error("waitForCompleted() was interrupted by exception: ", e);
    }
  }

  public void waitForFailed(long timeout, TimeUnit unit) {
    try {
      podFailedLatch.await(timeout, unit);
    } catch (InterruptedException e) {
      LOG.error("waitForCompleted() was interrupted by exception: ", e);
    }
  }

  public void waitForRunning(long timeout, TimeUnit unit) {
    try {
      podRunningLatch.await(timeout, unit);
    } catch (InterruptedException e) {
      LOG.error("waitForRunning() was interrupted by exception: ", e);
    }
  }

  public void waitForPending(long timeout, TimeUnit unit) {
    try {
      podPendingLatch.await(timeout, unit);
    } catch (InterruptedException e) {
      LOG.error("waitForPending() was interrupted by exception: ", e);
    }
  }

  private boolean isSucceeded() {
    if (pod.isPresent()) {
      phase = pod.get().getStatus().getPhase();
    }
    return phase == "Succeeded";
  }

  private boolean isFailed() {
    if (pod.isPresent()) {
      phase = pod.get().getStatus().getPhase();
    }
    return phase == "Failed";
  }

  private boolean isRunning() {
    if (pod.isPresent()) {
      phase = pod.get().getStatus().getPhase();
    }
    return phase == "Running";
  }

  private boolean isPending() {
    if (pod.isPresent()) {
      phase = pod.get().getStatus().getPhase();
    }
    return phase == "Pending";
  }

  private void closeWatchWhenRunning() {
    podRunningLatch.countDown();
  }

  private void closeWatchWhenPending() {
    podPendingLatch.countDown();
  }


  private void closeWatchWhenFailed() {
    podFailedLatch.countDown();
  }

  private void closeWatchWhenSucceed() {
    podSucceededLatch.countDown();
  }

  private void closeAllWatch() {
    closeWatchWhenFailed();
    closeWatchWhenSucceed();
    closeWatchWhenPending();
    closeWatchWhenRunning();
    closeWatchWhenFailed();
    closeWatchWhenSucceed();
  }
}
