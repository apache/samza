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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.AsyncStreamTaskApplication;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamTaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.system.StreamSpec;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;


/**
 * The primary means of managing execution of the {@link org.apache.samza.application.StreamApplication} at runtime.
 */
@InterfaceStability.Unstable
public abstract class ApplicationRunner {

  private static final String RUNNER_CONFIG = "app.runner.class";
  private static final String DEFAULT_RUNNER_CLASS = "org.apache.samza.runtime.RemoteApplicationRunner";

  protected final Config config;
  protected final Map<String, MetricsReporter> metricsReporters = new HashMap<>();

  /**
   * Static method to load the {@link ApplicationRunner}
   *
   * @param config  configuration passed in to initialize the Samza processes
   * @return  the configure-driven {@link ApplicationRunner} to run the user-defined stream applications
   */
  public static ApplicationRunner fromConfig(Config config) {
    try {
      Class<?> runnerClass = Class.forName(config.get(RUNNER_CONFIG, DEFAULT_RUNNER_CLASS));
      if (ApplicationRunner.class.isAssignableFrom(runnerClass)) {
        Constructor<?> constructor = runnerClass.getConstructor(Config.class); // *sigh*
        return (ApplicationRunner) constructor.newInstance(config);
      }
    } catch (Exception e) {
      throw new ConfigException(String.format("Problem in loading ApplicationRunner class %s", config.get(
          RUNNER_CONFIG)), e);
    }
    throw new ConfigException(String.format(
        "Class %s does not extend ApplicationRunner properly",
        config.get(RUNNER_CONFIG)));
  }


  ApplicationRunner(Config config) {
    if (config == null) {
      throw new NullPointerException("Parameter 'config' cannot be null.");
    }

    this.config = config;
  }

  /**
   * Deploy and run the Samza jobs to execute {@link org.apache.samza.task.StreamTask}.
   * It is non-blocking so it doesn't wait for the application running.
   * This method assumes you task.class is specified in the configs.
   *
   * NOTE. this interface will most likely change in the future.
   */
  @Deprecated
  @InterfaceStability.Evolving
  public abstract void runTask();


  /**
   * Deploy and run the Samza jobs to execute {@link StreamApplication}.
   * It is non-blocking so it doesn't wait for the application running.
   *
   * @param streamApp  the user-defined {@link StreamApplication} object
   */
  public abstract void run(ApplicationBase streamApp);

  /**
   * Kill the Samza jobs represented by {@link StreamApplication}
   * It is non-blocking so it doesn't wait for the application stopping.
   *
   * @param streamApp  the user-defined {@link StreamApplication} object
   */
  public abstract void kill(ApplicationBase streamApp);

  /**
   * Get the collective status of the Samza jobs represented by {@link StreamApplication}.
   * Returns {@link ApplicationRunner} running if all jobs are running.
   *
   * @param streamApp  the user-defined {@link StreamApplication} object
   * @return the status of the application
   */
  public abstract ApplicationStatus status(ApplicationBase streamApp);

  /**
   * Block until the local process for the application finishes
   */
  public abstract void waitForFinish();

  public abstract StreamGraph createGraph();

  public void setMetricsReporters(Map<String,MetricsReporter> metricsReporters) {
    this.metricsReporters.putAll(metricsReporters);
  }
}
