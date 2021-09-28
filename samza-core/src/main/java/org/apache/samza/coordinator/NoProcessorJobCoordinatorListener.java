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
package org.apache.samza.coordinator;

import java.util.concurrent.CountDownLatch;
import org.apache.samza.job.model.JobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link JobCoordinatorListener} for a {@link JobCoordinator} which does not run alongside a processor.
 */
public class NoProcessorJobCoordinatorListener implements JobCoordinatorListener {
  private static final Logger LOG = LoggerFactory.getLogger(NoProcessorJobCoordinatorListener.class);
  private final CountDownLatch waitForShutdownLatch;

  public NoProcessorJobCoordinatorListener(CountDownLatch waitForShutdownLatch) {
    this.waitForShutdownLatch = waitForShutdownLatch;
  }

  @Override
  public void onJobModelExpired() {
    // nothing to notify so far about job model expiration
  }

  @Override
  public void onNewJobModel(String processorId, JobModel jobModel) {
    // nothing to notify so far about new job model
  }

  @Override
  public void onCoordinatorStop() {
    this.waitForShutdownLatch.countDown();
  }

  /**
   * There is currently no use case for bubbling up this exception, so just log the error and allow shutdown for now. If
   * we get a future use case where it is useful to bubble up the exception, then we can update this class.
   */
  @Override
  public void onCoordinatorFailure(Throwable t) {
    LOG.error("Failure in coordinator, allowing shutdown to begin", t);
    this.waitForShutdownLatch.countDown();
  }
}
