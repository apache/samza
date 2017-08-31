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

import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.runtime.ApplicationRunner;


public class ApplicationBase {

  protected final ApplicationRunner runner;
  protected final Config config;

  ApplicationBase(ApplicationRunner runner, Config config) {
    this.runner = runner;
    this.config = config;
  }

  final void withMetricsReports(Map<String, MetricsReporter> reporterMap) {
    this.runner.addMetricsReporters(reporterMap);
  }

  /**
   * Deploy and run the Samza jobs to execute this application.
   * It is non-blocking so it doesn't wait for the application running.
   *
   */
  public final void run() {
    this.runner.run(this);
  }

  /**
   * Kill the Samza jobs represented by this application
   * It is non-blocking so it doesn't wait for the application stopping.
   *
   */
  public final void kill() {
    this.runner.kill(this);
  }

  /**
   * Get the collective status of the Samza jobs represented by this application.
   * Returns {@link ApplicationStatus} running if all jobs are running.
   *
   * @return the status of the application
   */
  public final ApplicationStatus status() {
    return this.runner.status(this);
  }

  /**
   * Method to wait for the runner in the current JVM process to finish.
   */
  public final void waitForFinish() {
    this.runner.waitForFinish(this);
  }

  protected void setContextFactory(ProcessorContextFactory factory) {
    // set the context factory object for the application
  }

  public class AppConfig extends MapConfig {

    public static final String APP_NAME = "app.name";
    public static final String APP_ID = "app.id";
    public static final String APP_CLASS = "app.class";

    public static final String JOB_NAME = "job.name";
    public static final String JOB_ID = "job.id";

    public AppConfig(Config config) {
      super(config);
    }

    public String getAppName() {
      return get(APP_NAME, get(JOB_NAME));
    }

    public String getAppId() {
      return get(APP_ID, get(JOB_ID, "1"));
    }

    public String getAppClass() {
      return get(APP_CLASS, null);
    }

    /**
     * returns full application id
     * @return full app id
     */
    public String getGlobalAppId() {
      return String.format("app-%s-%s", getAppName(), getAppId());
    }

  }


  public String getGlobalAppId() {
    return new AppConfig(config).getGlobalAppId();
  }
}
