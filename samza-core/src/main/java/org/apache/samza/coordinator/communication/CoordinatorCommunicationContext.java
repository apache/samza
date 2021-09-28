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
  private final JobInfoProvider jobInfoProvider;
  private final Config configForFactory;
  private final MetricsRegistry metricsRegistry;

  /**
   * @param jobInfoProvider provides dynamic access to job info to pass along to workers (e.g. job model, configs)
   * @param configForFactory config to use to build the {@link CoordinatorCommunication}
   */
  public CoordinatorCommunicationContext(JobInfoProvider jobInfoProvider, Config configForFactory,
      MetricsRegistry metricsRegistry) {
    this.configForFactory = configForFactory;
    this.jobInfoProvider = jobInfoProvider;
    this.metricsRegistry = metricsRegistry;
  }

  /**
   * Use this to access job model.
   */
  public JobInfoProvider getJobInfoProvider() {
    return jobInfoProvider;
  }

  /**
   * Use this to access the {@link Config} for building the {@link CoordinatorCommunication}.
   * Do not use this as the {@link Config} to pass along to the workers. Use {@link #getJobInfoProvider()} for accessing
   * the job model and config to pass along to workers.
   */
  public Config getConfigForFactory() {
    return configForFactory;
  }

  public MetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }
}
