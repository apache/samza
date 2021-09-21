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


/**
 * {@link JobCoordinatorListener} for a {@link JobCoordinator} which does not run alongside a processor.
 */
public class NoProcessorJobCoordinatorListener implements JobCoordinatorListener {
  private final CountDownLatch waitForShutdownLatch;

  public NoProcessorJobCoordinatorListener(CountDownLatch waitForShutdownLatch) {
    this.waitForShutdownLatch = waitForShutdownLatch;
  }

  @Override
  public void onJobModelExpired() {
    // nothing to do
  }

  @Override
  public void onNewJobModel(String processorId, JobModel jobModel) {
    // nothing to do
  }

  @Override
  public void onCoordinatorStop() {
    this.waitForShutdownLatch.countDown();
  }

  @Override
  public void onCoordinatorFailure(Throwable t) {
    this.waitForShutdownLatch.countDown();
  }
}
