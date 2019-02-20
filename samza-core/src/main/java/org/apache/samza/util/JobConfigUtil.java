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
package org.apache.samza.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobConfigUtil {
  private static final Logger LOG = LoggerFactory.getLogger(JobConfigUtil.class);

  /**
   * Generates job.id from app.id and job.name from app.name config
   * If both job.id and app.id is defined, app.id takes precedence and job.id is set to value of app.id
   * If both job.name and app.name is defined, app.name takes precedence and job.name is set to value of app.name
   *
   * @param userConfigs configs passed from user
   *
   */
  public static MapConfig generateJobIdAndName(MapConfig userConfigs) {
    Map<String, String> generatedConfig = new HashMap<>();

    if (userConfigs.containsKey(JobConfig.JOB_ID())) {
      LOG.warn("{} is a deprecated configuration, use app.id instead.", JobConfig.JOB_ID());
    }

    if (userConfigs.containsKey(JobConfig.JOB_NAME())) {
      LOG.warn("{} is a deprecated configuration, use use app.name instead.", JobConfig.JOB_NAME());
    }

    if (userConfigs.containsKey(ApplicationConfig.APP_NAME)) {
      String appName =  userConfigs.get(ApplicationConfig.APP_NAME);
      LOG.info("app.name is defined, generating job.name equal to app.name value: {}", appName);
      generatedConfig.put(JobConfig.JOB_NAME(), appName);
    }

    if (userConfigs.containsKey(ApplicationConfig.APP_ID)) {
      String appId =  userConfigs.get(ApplicationConfig.APP_ID);
      LOG.info("app.id is defined, generating job.id equal to app.name value: {}", appId);
      generatedConfig.put(JobConfig.JOB_ID(), appId);
    }

    return new MapConfig(generatedConfig);
  }

}
