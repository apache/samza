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
package org.apache.samza.monitor;

import org.apache.samza.metrics.MetricsRegistry;

/**
 * Factory to build {@link org.apache.samza.monitor.Monitor} using provided config.
 */
public interface MonitorFactory {

  /**
   * @param config contains the configuration defined for the monitor.
   * @param metricsRegistry instance that will allow the monitor
   *                        implementations to register custom metrics
   * @return Constructs and returns the monitor instance.
   * @throws Exception if there was any problem with instantiating the monitor.
   */
  Monitor getMonitorInstance(MonitorConfig config, MetricsRegistry metricsRegistry)
    throws Exception;
}
