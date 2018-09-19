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


public class JobContextImpl implements JobContext {
  private final Config config;
  private final String jobName;
  private final String jobId;

  public JobContextImpl(Config config, String jobName, String jobId) {
    this.config = config;
    this.jobName = jobName;
    this.jobId = jobId;
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
}
