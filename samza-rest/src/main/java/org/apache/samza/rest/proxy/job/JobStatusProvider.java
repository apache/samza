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
import java.util.Collection;
import org.apache.samza.rest.model.Job;


/**
 * Interface for getting job status independent of the underlying cluster implementation.
 */
public interface JobStatusProvider {
  /**
   * Populates the status* fields of each {@link Job} in the provided Collection.
   *
   * @param jobs                  the collection of {@link Job} for which the status is needed.
   * @throws IOException          if there was a problem executing the command to get the status.
   * @throws InterruptedException if the thread was interrupted while waiting for the status result.
   */
  void getJobStatuses(Collection<Job> jobs)
      throws IOException, InterruptedException;

  /**
   * @param jobInstance           the instance of the job.
   * @return                      a {@link Job} containing
   *                              the status for the job specified by jobName and jobId.
   * @throws IOException          if there was a problem executing the command to get the status.
   * @throws InterruptedException if the thread was interrupted while waiting for the status result.
   */
  Job getJobStatus(JobInstance jobInstance)
      throws IOException, InterruptedException;
}
