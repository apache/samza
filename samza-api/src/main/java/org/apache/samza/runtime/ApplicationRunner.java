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

import java.time.Duration;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.system.StreamSpec;

import java.lang.reflect.Constructor;


/**
 * The primary means of managing execution of the {@link org.apache.samza.application.StreamApplication} at runtime.
 */
@InterfaceStability.Unstable
public abstract class ApplicationRunner {

  private static final String RUNNER_CONFIG = "app.runner.class";
  private static final String DEFAULT_RUNNER_CLASS = "org.apache.samza.runtime.RemoteApplicationRunner";

  protected final Config config;

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


  public ApplicationRunner(Config config) {
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
  @InterfaceStability.Evolving
  public abstract void runTask();


  /**
   * Deploy and run the Samza jobs to execute {@link StreamApplication}.
   * It is non-blocking so it doesn't wait for the application running.
   *
   * @param streamApp  the user-defined {@link StreamApplication} object
   */
  public abstract void run(StreamApplication streamApp);

  /**
   * Kill the Samza jobs represented by {@link StreamApplication}
   * It is non-blocking so it doesn't wait for the application stopping.
   *
   * @param streamApp  the user-defined {@link StreamApplication} object
   */
  public abstract void kill(StreamApplication streamApp);

  /**
   * Get the collective status of the Samza jobs represented by {@link StreamApplication}.
   * Returns {@link ApplicationRunner} running if all jobs are running.
   *
   * @param streamApp  the user-defined {@link StreamApplication} object
   * @return the status of the application
   */
  public abstract ApplicationStatus status(StreamApplication streamApp);

  /**
   * Waits until the application finishes.
   */
  public void waitForFinish() {
    throw new UnsupportedOperationException(getClass().getName() + " does not support waitForFinish.");
  }

  /**
   * Waits for the application to finish. It times out after the input duration has elapsed.
   *
   * @param timeout time to wait for the application to finish
   */
  public void waitForFinish(Duration timeout) {
    throw new UnsupportedOperationException(getClass().getName() + " does not support waitForFinish.");
  }

  /**
   * Constructs a {@link StreamSpec} from the configuration for the specified streamId.
   *
   * The stream configurations are read from the following properties in the config:
   * {@code streams.{$streamId}.*}
   * <br>
   * All properties matching this pattern are assumed to be system-specific with two exceptions. The following two
   * properties are Samza properties which are used to bind the stream to a system and a physical resource on that system.
   *
   * <ul>
   *   <li>samza.system -         The name of the System on which this stream will be used. If this property isn't defined
   *                              the stream will be associated with the System defined in {@code job.default.system}</li>
   *   <li>samza.physical.name -  The system-specific name for this stream. It could be a file URN, topic name, or other identifer.
   *                              If this property isn't defined the physical.name will be set to the streamId</li>
   * </ul>
   *
   * @param streamId  The logical identifier for the stream in Samza.
   * @return          The {@link StreamSpec} instance.
   */
  public abstract StreamSpec getStreamSpec(String streamId);
}
