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
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.runtime.ApplicationRunner;


public class ApplicationBase {

  protected final ApplicationRunner runner;

  ApplicationBase(ApplicationRunner runner) {
    this.runner = runner;
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
    this.runner.waitForFinish();
  }

}
