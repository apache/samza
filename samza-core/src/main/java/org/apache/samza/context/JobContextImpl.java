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
package org.apache.samza.context;

import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.job.model.JobModel;


public class JobContextImpl implements JobContext {
  private final Config config;
  private final JobModel jobModel;
  private final String jobName;
  private final String jobId;

  private JobContextImpl(Config config, String jobName, String jobId, JobModel jobModel) {
    this.config = config;
    this.jobName = jobName;
    this.jobId = jobId;
    this.jobModel = jobModel;
  }

  /**
   * Build a {@link JobContextImpl} from a {@link Config} object.
   * This extracts some information like job name and job id.
   *
   * @param config used to extract job information
   * @param jobModel job model
   * @return {@link JobContextImpl} corresponding to the {@code config}
   * @throws IllegalArgumentException if job name is not defined in the {@code config}
   */
  public static JobContextImpl fromConfigWithDefaults(Config config, JobModel jobModel) {
    JobConfig jobConfig = new JobConfig(config);
    String jobName = jobConfig.getName()
        .orElseThrow(() -> new IllegalArgumentException(String.format("Config %s is missing", JobConfig.JOB_NAME)));
    String jobId = jobConfig.getJobId();
    return new JobContextImpl(config, jobName, jobId, jobModel);
  }

  @Override
  public Config getConfig() {
    return this.config;
  }

  @Override
  public String getJobName() {
    return this.jobName;
  }

  @Override
  public String getJobId() {
    return this.jobId;
  }

  @Override
  public JobModel getJobModel() {
    return this.jobModel;
  }
}
