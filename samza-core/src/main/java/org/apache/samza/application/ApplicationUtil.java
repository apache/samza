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
package org.apache.samza.application;

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.TaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Util class to create {@link SamzaApplication} from the configuration.
 */
public class ApplicationUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationUtil.class);

  /**
   * Creates the {@link SamzaApplication} object from the task or application class name specified in {@code config}
   *
   * @param config the configuration of the application
   * @return the {@link SamzaApplication} object
   */
  public static SamzaApplication fromConfig(Config config) {

    String appClassName = new ApplicationConfig(config).getAppClass();
    if (StringUtils.isNotBlank(appClassName)) {
      // app.class is configured
      try {
        Class<SamzaApplication> appClass = (Class<SamzaApplication>) Class.forName(appClassName);
        if (StreamApplication.class.isAssignableFrom(appClass) || TaskApplication.class.isAssignableFrom(appClass)) {
          return appClass.newInstance();
        }
      } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
        throw new ConfigException(String.format("Loading app.class %s failed. The user application has to implement "
            + "StreamApplication or TaskApplication.", appClassName), e);
      }
    }
    // no app.class defined. It has to be a legacy application with task.class configuration
    TaskConfig taskConfig = new TaskConfig(config);
    Optional<String> taskClassOption = taskConfig.getTaskClass();
    if (!taskClassOption.isPresent() || StringUtils.isBlank(taskClassOption.get())) {
      // no task.class defined either. This is wrong.
      throw new ConfigException("Legacy task applications must set a non-empty task.class in configuration.");
    }
    return new LegacyTaskApplication(taskClassOption.get());
  }

  /**
   * Determines if the job is a Samza high-level job.
   * @param config config
   * */
  public static boolean isHighLevelApiJob(Config config) {
    final ApplicationConfig applicationConfig = new ApplicationConfig(config);
    return applicationConfig.getAppApiType() == ApplicationApiType.HIGH_LEVEL;
  }
}
