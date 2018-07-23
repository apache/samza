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
package org.apache.samza.runtime.internal;

import java.time.Duration;
import java.util.Map;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.application.ApplicationSpec;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;


/**
 * The primary means of managing execution of user applications defined in {@link ApplicationSpec} at runtime.
 */
@InterfaceStability.Evolving
public interface ApplicationRunner {

  void run(ApplicationSpec appSpec);

  void kill(ApplicationSpec appSpec);

  ApplicationStatus status(ApplicationSpec appSpec);

  @Deprecated
  void waitForFinish(ApplicationSpec appSpec);

  /**
   * Waits for {@code timeout} duration for the application to finish.
   *
   * @param timeout time to wait for the application to finish
   * @return true - application finished before timeout
   *         false - otherwise
   */
  boolean waitForFinish(ApplicationSpec appSpec, Duration timeout);

  /**
   * Method to add a set of customized {@link MetricsReporter}s in the application
   *
   * @param metricsReporters the map of customized {@link MetricsReporter}s objects to be used
   */
  void addMetricsReporters(Map<String, MetricsReporter> metricsReporters);

}
