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
package org.apache.samza.runtime;

import java.lang.reflect.Constructor;
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.internal.AppDescriptorImpl;
import org.apache.samza.application.internal.StreamAppDescriptorImpl;
import org.apache.samza.application.internal.TaskAppDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;


/**
 * Creates {@link ApplicationRunner} instances based on configuration and user-implemented {@link ApplicationBase}
 */
public class ApplicationRunners {

  private ApplicationRunners() {

  }

  /**
   * Get the {@link ApplicationRunner} that runs the {@code userApp}
   *
   * @param userApp the user application object
   * @param config the configuration for this application
   * @return the {@link ApplicationRunner} object that will run the {@code userApp}
   */
  public static final ApplicationRunner getApplicationRunner(ApplicationBase userApp, Config config) {
    if (userApp instanceof StreamApplication) {
      return getRunner(new StreamAppDescriptorImpl((StreamApplication) userApp, config));
    }
    if (userApp instanceof TaskApplication) {
      return getRunner(new TaskAppDescriptorImpl((TaskApplication) userApp, config));
    }
    throw new IllegalArgumentException(String.format("User application instance has to be either StreamApplicationFactory or TaskApplicationFactory. "
        + "Invalid userApp class %s.", userApp.getClass().getName()));
  }

  static class AppRunnerConfig {
    private static final String APP_RUNNER_CFG = "app.runner.class";
    private static final String DEFAULT_APP_RUNNER = "org.apache.samza.runtime.RemoteApplicationRunner";

    private final Config config;

    AppRunnerConfig(Config config) {
      this.config = config;
    }

    String getAppRunnerClass() {
      return this.config.getOrDefault(APP_RUNNER_CFG, DEFAULT_APP_RUNNER);
    }

  }

  /**
   * Static method to get the {@link ApplicationRunner}
   *
   * @param appSpec  configuration passed in to initialize the Samza processes
   * @return  the configure-driven {@link ApplicationRunner} to run the user-defined stream applications
   */
  static ApplicationRunner getRunner(AppDescriptorImpl appSpec) {
    AppRunnerConfig appRunnerCfg = new AppRunnerConfig(appSpec.getConfig());
    try {
      Class<?> runnerClass = Class.forName(appRunnerCfg.getAppRunnerClass());
      if (ApplicationRunner.class.isAssignableFrom(runnerClass)) {
        Constructor<?> constructor = runnerClass.getConstructor(AppDescriptorImpl.class); // *sigh*
        return (ApplicationRunner) constructor.newInstance(appSpec);
      }
    } catch (Exception e) {
      throw new ConfigException(String.format("Problem in loading ApplicationRunner class %s",
          appRunnerCfg.getAppRunnerClass()), e);
    }
    throw new ConfigException(String.format(
        "Class %s does not extend ApplicationRunner properly",
        appRunnerCfg.getAppRunnerClass()));
  }
}
