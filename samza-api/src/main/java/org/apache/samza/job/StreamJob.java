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

package org.apache.samza.job;

/**
 * A StreamJob runs Samza {@link org.apache.samza.task.StreamTask}s in its specific environment.
 * Users generally do not need to implement a StreamJob themselves, rather it is a framework-level
 * interface meant for those extending Samza itself.  This class, and its accompanying factory,
 * allow Samza to run on other service providers besides YARN and LocalJob, such as Mesos or Sun Grid Engine.
 */
public interface StreamJob {
  /**
   * Submit this job to be run.
   * @return An instance of this job after it has been submitted.
   */
  StreamJob submit();

  /**
   * Kill this job immediately.
   *
   * @return An instance of this job after it has been killed.
   */
  StreamJob kill();

  /**
   * Block on this job until either it finishes or reaches its timeout value
   *
   * @param timeoutMs How many milliseconds to wait before returning, assuming the job has not yet finished
   * @return {@link org.apache.samza.job.ApplicationStatus} of the job after finishing or timing out
   */
  ApplicationStatus waitForFinish(long timeoutMs);

  /**
   * Block on this job until either it transitions to the specified status or reaches it timeout value
   *
   * @param status Target {@link org.apache.samza.job.ApplicationStatus} to wait upon
   * @param timeoutMs How many milliseconds to wait before returning, assuming the job has not transitioned to the specified value
   * @return {@link org.apache.samza.job.ApplicationStatus} of the job after finishing or reaching target state
   */
  ApplicationStatus waitForStatus(ApplicationStatus status, long timeoutMs);

  /**
   * Get current {@link org.apache.samza.job.ApplicationStatus} of the job
   * @return Current job status
   */
  ApplicationStatus getStatus();
}
