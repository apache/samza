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
package org.apache.samza.rest.model;

import org.codehaus.jackson.annotate.JsonProperty;


/**
 * The client view of a job. Includes the job name, id, and status.
 *
 * The minimum goal is to provide enough information to execute REST commands on that job, so the name and id are required.
 */
public class Job {
  private String jobName;
  private String jobId;
  private JobStatus status = JobStatus.UNKNOWN; // started, stopped, or starting
  private String statusDetail; // Detailed status e.g. for YARN it could be ACCEPTED, RUNNING, etc.

  public Job(String jobName, String jobId) {
    this.jobName = jobName;
    this.jobId = jobId;
  }

  public Job(@JsonProperty("jobName") String jobName, @JsonProperty("jobId")String jobId, @JsonProperty("status") JobStatus status, @JsonProperty("statusDetail") String statusDetail) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.status = status;
    this.statusDetail = statusDetail;
  }

  public Job() {
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public JobStatus getStatus() {
    return status;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
  }

  public String getStatusDetail() {
    return statusDetail;
  }

  public void setStatusDetail(String statusDetail) {
    this.statusDetail = statusDetail;
  }
}
