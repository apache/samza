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

import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.task.TaskFactoryUtil;

import static org.apache.samza.util.ScalaJavaUtil.*;


/**
 * Util class to create {@link ApplicationBase} from the configuration.
 */
public class ApplicationClassUtils {

  /**
   * Creates the {@link ApplicationBase} object from the {@code config}
   *
   * @param config the configuration of the application
   * @return the {@link ApplicationBase} object
   */
  public static ApplicationBase fromConfig(Config config) {
    ApplicationConfig appConfig = new ApplicationConfig(config);
    if (appConfig.getAppClass() != null && !appConfig.getAppClass().isEmpty()) {
      // app.class is configured
      try {
        Class<ApplicationBase> appClass = (Class<ApplicationBase>) Class.forName(appConfig.getAppClass());
        if (StreamApplication.class.isAssignableFrom(appClass) || TaskApplication.class.isAssignableFrom(appClass)) {
          return appClass.newInstance();
        }
      } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
        throw new ConfigException(String.format("Loading app.class %s failed. The user application has to implement "
            + "StreamApplication or TaskApplication.", appConfig.getAppClass()), e);
      }
    }
    // no app.class defined. It has to be a legacy application with task.class configuration
    new TaskConfig(config).getTaskClass().getOrElse(toScalaFunction(
        () -> {
        throw new ConfigException("No task class defined in the configuration.");
      }));
    return (TaskApplication) (appSpec) -> appSpec.setTaskFactory(TaskFactoryUtil.createTaskFactory(config));
  }

}
