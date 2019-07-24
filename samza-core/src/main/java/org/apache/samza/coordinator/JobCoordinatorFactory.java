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
package org.apache.samza.coordinator;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;

@InterfaceStability.Evolving
public interface JobCoordinatorFactory {
  /**
   * Returns a new instance of {@link JobCoordinator}.
   * @param processorId a unique logical identifier assigned to the {@link org.apache.samza.processor.StreamProcessor}.
   * @param config the configuration of the samza application.
   * @param metricsRegistry  used to publish the coordination specific metrics.
   * @param metadataStore used to read and write metadata for the samza application.
   * @return the {@link JobCoordinator} instance.
   */
  JobCoordinator getJobCoordinator(String processorId, Config config, MetricsRegistry metricsRegistry,
      MetadataStore metadataStore);
}