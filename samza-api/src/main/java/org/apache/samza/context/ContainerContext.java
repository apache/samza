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
package org.apache.samza.context;

import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.metrics.MetricsRegistry;


/**
 * Contains information at container granularity, provided by the Samza framework, to be used to instantiate an
 * application at runtime.
 * <p>
 * Note that application-defined container-level context is accessible through
 * {@link ApplicationContainerContext}.
 */
public interface ContainerContext {
  /**
   * Returns the {@link ContainerModel} associated with this container. This contains information like the id and the
   * associated {@link org.apache.samza.job.model.TaskModel}s.
   * @return {@link ContainerModel} associated with this container
   */
  ContainerModel getContainerModel();

  /**
   * Returns the {@link MetricsRegistry} for this container. Metrics built using this registry will be associated with
   * the container.
   * @return {@link MetricsRegistry} for this container
   */
  MetricsRegistry getContainerMetricsRegistry();
}
