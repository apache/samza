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

public class JavaJobConfig extends MapConfig {
  private static final String JOB_NAME = "job.name"; // streaming.job_name
  private static final String JOB_ID = "job.id"; // streaming.job_id
  private static final String DEFAULT_JOB_ID = "1";

  public JavaJobConfig(Config config) {
    super(config);
  }

  public String getJobName() {
    if (!containsKey(JOB_NAME)) {
      throw new ConfigException("Missing " + JOB_NAME + " config!");
    }
    return get(JOB_NAME);
  }

  public String getJobName(String defaultValue) {
    return get(JOB_NAME, defaultValue);
  }

  public String getJobId() {
    return get(JOB_ID, DEFAULT_JOB_ID);
  }

}
