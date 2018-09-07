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
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;


/**
 * Creates {@link ApplicationRunner} instances based on configuration and user-implemented {@link SamzaApplication}
 *
 * <p> This class is usually used in main() function to create an instance of {@link ApplicationRunner}, as in the example
 * below:
 * <pre>{@code
 *   public static void main(String[] args) {
 *     CommandLine cmdLine = new CommandLine();
 *     Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
 *     PageViewCounter app = new PageViewCounter();
 *     ApplicationRunner runner = ApplicationRunners.getApplicationRunner(app, config);
 *     runner.run();
 *     runner.waitForFinish();
 *   }
 * }</pre>
 */
public class ApplicationRunners {

  private static final String APP_RUNNER_CFG = "app.runner.class";
  private static final String DEFAULT_APP_RUNNER = "org.apache.samza.runtime.RemoteApplicationRunner";

  private ApplicationRunners() {

  }

  /**
   * Get the {@link ApplicationRunner} that runs the {@code userApp}
   *
   * @param userApp the user application object
   * @param config the configuration for this application
   * @return the {@link ApplicationRunner} object that will run the {@code userApp}
   */
  public static final ApplicationRunner getApplicationRunner(SamzaApplication userApp, Config config) {
    String appRunnerClassName = getAppRunnerClass(config);
    try {
      Class<?> runnerClass = Class.forName(appRunnerClassName);
      if (!ApplicationRunner.class.isAssignableFrom(runnerClass)) {
        throw new ConfigException(
            String.format("Class %s does not extend ApplicationRunner properly", appRunnerClassName));
      }
      Constructor<?> constructor = runnerClass.getConstructor(SamzaApplication.class, Config.class); // *sigh*
      return (ApplicationRunner) constructor.newInstance(userApp, config);
    } catch (ConfigException ce) {
      // this is thrown due to invalid app.runner.class configuration
      throw ce;
    } catch (Exception e) {
      // other types of exception during class loading and construction of new instance
      throw new ConfigException(String.format("Could not load ApplicationRunner class %s", appRunnerClassName), e);
    }
  }

  private static String getAppRunnerClass(Config config) {
    return config.getOrDefault(APP_RUNNER_CFG, DEFAULT_APP_RUNNER);
  }

}
