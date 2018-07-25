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

package org.apache.samza.storage;

import java.io.Serializable;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;


/**
 * A factory to build {@link SideInputsProcessor}s.
 *
 * Implementations should return a new instance for every invocation of
 * {@link #getSideInputsProcessor(Config, MetricsRegistry)}
 */
@FunctionalInterface
@InterfaceStability.Unstable
public interface SideInputsProcessorFactory extends Serializable {
  /**
   * Creates a new instance of a {@link SideInputsProcessor}.
   *
   * @param config the configuration
   * @param metricsRegistry the metrics registry
   * @return an instance of {@link SideInputsProcessor}
   */
  SideInputsProcessor getSideInputsProcessor(Config config, MetricsRegistry metricsRegistry);
}
