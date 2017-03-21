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
package org.apache.samza.processor;

import org.apache.samza.config.Config;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsReporter;
import scala.collection.immutable.Map;

public class SamzaContainerWrapper {
  private SamzaContainerWrapper() {

  }

  public static SamzaContainer createInstance(
      int containerId,
      ContainerModel containerModel,
      Config config,
      int maxChangeLogStreamPartitions,
      LocalityManager localityManager,
      JmxServer jmxServer,
      Map<String, MetricsReporter> customReporters,
      Object taskFactory) {
    return SamzaContainer.apply(
        containerId, containerModel, config, maxChangeLogStreamPartitions, localityManager, jmxServer,
        customReporters, taskFactory);
  }

  public static LocalityManager getLocalityManager(int containerId, Config config) {
    return SamzaContainer.getLocalityManager(containerId, config);
  }
}
