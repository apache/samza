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
package org.apache.samza.config;

import java.util.concurrent.TimeUnit;


/**
 * A container class to hold run loop related configurations to prevent constructor explosion
 * in {@link org.apache.samza.container.RunLoop}
 */
public class RunLoopConfig extends MapConfig {
  private static final String CONTAINER_DISK_QUOTA_DELAY_MAX_MS = "container.disk.quota.delay.max.ms";
  private ApplicationConfig appConfig;
  private JobConfig jobConfig;
  private TaskConfig taskConfig;

  public RunLoopConfig(Config config) {
    super(config);
    this.appConfig = new ApplicationConfig(config);
    this.jobConfig = new JobConfig(config);
    this.taskConfig = new TaskConfig(config);
  }

  public int getMaxConcurrency() {
    return taskConfig.getMaxConcurrency();
  }

  public long getTaskCallbackTimeoutMs() {
    return taskConfig.getCallbackTimeoutMs();
  }

  public long getDrainCallbackTimeoutMs() {
    return taskConfig.getDrainCallbackTimeoutMs();
  }

  public long getWatermarkCallbackTimeoutMs() {
    return taskConfig.getWatermarkCallbackTimeoutMs();
  }

  public boolean asyncCommitEnabled() {
    return taskConfig.getAsyncCommit();
  }

  public long getWindowMs() {
    return taskConfig.getWindowMs();
  }

  public long getCommitMs() {
    return taskConfig.getCommitMs();
  }

  public long getMaxIdleMs() {
    return taskConfig.getMaxIdleMs();
  }

  public long getMaxThrottlingDelayMs() {
    return getLong(CONTAINER_DISK_QUOTA_DELAY_MAX_MS, TimeUnit.SECONDS.toMillis(1));
  }

  public String getRunId() {
    return appConfig.getRunId();
  }

  public int getElasticityFactor() {
    return jobConfig.getElasticityFactor();
  }

  public boolean isHighLevelApiJob() {
    return appConfig.isHighLevelApiJob();
  }
}