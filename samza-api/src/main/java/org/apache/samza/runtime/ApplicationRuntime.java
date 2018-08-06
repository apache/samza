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
import java.util.Map;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;


/**
 * The primary execution methods of a runtime instance of the user application.
 */
public interface ApplicationRuntime {
  /**
   * Start a runtime instance of the application
   */
  void run();

  /**
   * Stop a runtime instance of the application
   */
  void kill();

  /**
   * Get the {@link ApplicationStatus} of a runtime instance of the application
   * @return the runtime status of the application
   */
  ApplicationStatus status();

  /**
   * Wait the runtime instance of the application to complete.
   * This method will block until the application completes.
   */
  void waitForFinish();

  /**
   * Wait the runtime instance of the application to complete with a {@code timeout}
   *
   * @param timeout the time to block to wait for the application to complete
   * @return true if the application completes within timeout; false otherwise
   */
  boolean waitForFinish(Duration timeout);

  /**
   * Method to add a set of customized {@link MetricsReporter}s in the application runtime instance
   *
   * @param metricsReporters the map of customized {@link MetricsReporter}s objects to be used
   */
  void addMetricsReporters(Map<String, MetricsReporter> metricsReporters);

}
