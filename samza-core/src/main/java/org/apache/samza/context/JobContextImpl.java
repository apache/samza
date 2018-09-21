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

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;


public class JobContextImpl implements JobContext {
  private final Config config;
  private final String jobName;
  private final String jobId;

  /**
   * @param config config for the job
   * @param jobName nullable job name (possible if job.name config is not set)
   * @param jobId id for the job
   */
  private JobContextImpl(Config config, String jobName, String jobId) {
    this.config = config;
    this.jobName = jobName;
    this.jobId = jobId;
  }

  public static JobContextImpl fromConfigWithDefaults(Config config) {
    JobConfig jobConfig = new JobConfig(config);
    String jobName = jobConfig.getName().isDefined() ? jobConfig.getName().get() : null;
    String jobId = jobConfig.getJobId().isDefined() ? jobConfig.getJobId().get() : "1";
    return new JobContextImpl(config, jobName, jobId);
  }

  @Override
  public Config getConfig() {
    return this.config;
  }

  @Override
  public String getJobName() {
    if (this.jobName == null) {
      throw new SamzaException("Job name was not specified. Check if the job.name configuration is missing.");
    }
    return this.jobName;
  }

  @Override
  public String getJobId() {
    return this.jobId;
  }
}
