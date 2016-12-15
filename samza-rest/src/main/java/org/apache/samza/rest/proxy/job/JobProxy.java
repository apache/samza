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
package org.apache.samza.rest.proxy.job;

import java.io.IOException;
import java.util.List;
import org.apache.samza.rest.model.Job;
import org.apache.samza.rest.model.JobStatus;


/**
 * Job proxy is the primary abstraction used by the REST API to interact with jobs.
 *
 * Concrete implementations of this interface may vary in the APIs they use to discover, start, stop, and stat jobs,
 * how to interact with the installed jobs on disk, etc.
 */
public interface JobProxy {
  /**
   * @param jobInstance the instance of the job
   * @return            true if the job exists and can be started, stopped, etc.
   */
  boolean jobExists(JobInstance jobInstance);

  /**
   * @return                      a {@link Job} for each Samza job instance installed on this host.
   * @throws IOException          if there was a problem executing the command to get the status.
   * @throws InterruptedException if the thread was interrupted while waiting for the status result.
   */
  List<Job> getAllJobStatuses()
      throws IOException, InterruptedException;

  /**
   * @param jobInstance           the instance of the job for which the status is needed.
   * @return                      a {@link Job} containing
   *                              the status for the job specified by jobName and jobId.
   * @throws IOException          if there was a problem executing the command to get the status.
   * @throws InterruptedException if the thread was interrupted while waiting for the status result.
   */
  Job getJobStatus(JobInstance jobInstance)
      throws IOException, InterruptedException;

  /**
   * Starts the job instance specified by jobName and jobId. When this method returns, the status of the job
   * should be {@link JobStatus#STARTING} or
   * {@link JobStatus#STARTED} depending on the implementation.
   *
   * @param jobInstance the instance of the job to start.
   * @throws Exception  if the job could not be successfully started.
   */
  void start(JobInstance jobInstance)
      throws Exception;

  /**
   * Stops the job instance specified by jobName and jobId. When this method returns, the status of the job
   * should be {@link JobStatus#STOPPED}.
   *
   * @param jobInstance the instance of the job to stop.
   * @throws Exception  if the job could not be successfully stopped.
   */
  void stop(JobInstance jobInstance)
      throws Exception;
}
