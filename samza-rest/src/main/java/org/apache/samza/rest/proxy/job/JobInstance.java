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

import com.google.common.base.Preconditions;


/**
 * Allows us to encapsulate the jobName,jobId tuple as one entity.
 */
public class JobInstance {

  private final String jobName;
  private final String jobId;

  /**
   * Required constructor.
   *
   * @param jobName the name of the job.
   * @param jobId   the id of the job.
   */
  public JobInstance(String jobName, String jobId) {
    this.jobName = Preconditions.checkNotNull(jobName);
    this.jobId = Preconditions.checkNotNull(jobId);
  }

  /**
   * @return  the name of the job.
   */
  public String getJobName() {
    return jobName;
  }

  /**
   * @return  the id of the job.
   */
  public String getJobId() {
    return jobId;
  }

  @Override
  public int hashCode() {
    final int prime = 139;
    int hash = prime * jobName.hashCode() + prime * jobId.hashCode();
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof JobInstance)) {
      return false;
    }

    JobInstance otherJob = (JobInstance) other;
    return this.jobName.equals(otherJob.jobName) && this.jobId.equals(otherJob.jobId);
  }

  @Override
  public String toString() {
    return String.format("jobName:%s jobId:%s", jobName, jobId);
  }
}
