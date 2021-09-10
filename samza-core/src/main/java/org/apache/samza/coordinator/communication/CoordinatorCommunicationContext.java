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
package org.apache.samza.coordinator.communication;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;


/**
 * Contains components which can be used to build a {@link CoordinatorCommunication}.
 * For example, provides access to job model and handling for worker states.
 */
public class CoordinatorCommunicationContext {
  private final JobModelProvider jobModelProvider;
  private final Config config;
  private final MetricsRegistry metricsRegistry;

  public CoordinatorCommunicationContext(JobModelProvider jobModelProvider, Config config,
      MetricsRegistry metricsRegistry) {
    this.config = config;
    this.jobModelProvider = jobModelProvider;
    this.metricsRegistry = metricsRegistry;
  }

  /**
   * Use this to access job model.
   */
  public JobModelProvider getJobModelProvider() {
    return jobModelProvider;
  }

  public Config getConfig() {
    return config;
  }

  public MetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }
}
