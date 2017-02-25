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
package org.apache.samza.monitor.mock;

import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.monitor.Monitor;
import org.apache.samza.monitor.MonitorConfig;
import org.apache.samza.monitor.MonitorFactory;
import org.mockito.Mockito;


public class MockMonitorFactory implements MonitorFactory {

  public static final Monitor MOCK_MONITOR = Mockito.mock(Monitor.class);

  @Override
  public Monitor getMonitorInstance(String monitorName, MonitorConfig config, MetricsRegistry metricsRegistry)
      throws Exception {
    Mockito.reset(MOCK_MONITOR);
    return MOCK_MONITOR;
  }
}
