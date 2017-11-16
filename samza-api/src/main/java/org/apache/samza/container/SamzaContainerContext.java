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

package org.apache.samza.container;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;

import java.util.Collection;
import java.util.Collections;

/**
 * A SamzaContainerContext maintains per-container information for the tasks it executes.
 */
public class SamzaContainerContext {
  public final String id;
  public final Config config;
  public final Collection<TaskName> taskNames;
  public final MetricsRegistry metricsRegistry;

  /**
   * An immutable context object that can passed to tasks to give them information
   * about the container in which they are executing.
   * @param id The id of the container.
   * @param config The job configuration.
   * @param taskNames The set of taskName keys for which this container is responsible.
   * @param metricsRegistry the {@link MetricsRegistry} for the container metrics
   */
  public SamzaContainerContext(
      String id,
      Config config,
      Collection<TaskName> taskNames,
      MetricsRegistry metricsRegistry) {
    this.id = id;
    this.config = config;
    this.taskNames = Collections.unmodifiableCollection(taskNames);
    this.metricsRegistry = metricsRegistry;
  }

  /**
   * See also {@link SamzaContainerContext#SamzaContainerContext(String, Config, Collection, MetricsRegistry)}.
   * NOTE: Used for testing only.
   * @param id The id of the container.
   * @param config The job configuration.
   * @param taskNames The set of taskName keys for which this container is responsible.
   */
  public SamzaContainerContext(
      String id,
      Config config,
      Collection<TaskName> taskNames) {
    this(id, config, taskNames, null);
  }
}
