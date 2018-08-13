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
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;


/**
 * The primary means of managing execution of the {@link org.apache.samza.application.ApplicationBase} at runtime.
 */
@InterfaceStability.Evolving
public interface ApplicationRunner {
  /**
   * Deploy and run the Samza jobs to execute {@link org.apache.samza.application.ApplicationBase}.
   * It is non-blocking so it doesn't wait for the application running.
   */
  void run();

  /**
   * Kill the Samza jobs represented by {@link org.apache.samza.application.ApplicationBase}
   * It is non-blocking so it doesn't wait for the application stopping.
   */
  void kill();

  /**
   * Get the collective status of the Samza jobs represented by {@link org.apache.samza.application.ApplicationBase}.
   * Returns {@link ApplicationStatus} object.
   *
   * @return the current status of an instance of {@link org.apache.samza.application.ApplicationBase}
   */
  ApplicationStatus status();

  /**
   * Waits until the application finishes.
   */
  void waitForFinish();

  /**
   * Waits for {@code timeout} duration for the application to finish.
   *
   * @param timeout time to wait for the application to finish
   * @return true - application finished before timeout
   *         false - otherwise
   */
  boolean waitForFinish(Duration timeout);

  /**
   * Add a set of customized {@link MetricsReporter}s in the application
   *
   * @param metricsReporters the map of customized {@link MetricsReporter}s objects to be used
   */
  void addMetricsReporters(Map<String, MetricsReporter> metricsReporters);

}
