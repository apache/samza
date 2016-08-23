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
package org.apache.samza.rest.resources.mock;

import java.io.IOException;
import java.util.Collection;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.rest.proxy.job.JobStatusProvider;
import org.apache.samza.rest.model.Job;
import org.apache.samza.rest.model.JobStatus;


public class MockJobStatusProvider implements JobStatusProvider {
  @Override
  public void getJobStatuses(Collection<Job> jobs)
      throws IOException, InterruptedException {
     for (Job info : jobs) {
       setStatusStarted(info);
     }
  }

  @Override
  public Job getJobStatus(JobInstance jobInstance)
      throws IOException, InterruptedException {
    Job info = new Job(jobInstance.getJobName(), jobInstance.getJobId());
    setStatusStarted(info);
    return info;
  }

  private void setStatusStarted(Job info) {
    info.setStatus(JobStatus.STARTED);
    info.setStatusDetail("RUNNING");
  }
}
