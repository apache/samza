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
package org.apache.samza.startpoint;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metadatastore.InMemoryMetadataStoreFactory;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.util.NoOpMetricsRegistry;


public class StartpointManagerTestUtil {
  private final MetadataStore metadataStore;
  private final StartpointManager startpointManager;

  public StartpointManagerTestUtil() {
    this(new InMemoryMetadataStoreFactory(), new MapConfig(), new NoOpMetricsRegistry());
  }

  public StartpointManagerTestUtil(MetadataStoreFactory metadataStoreFactory, Config config, MetricsRegistry metricsRegistry) {
    this.metadataStore = metadataStoreFactory.getMetadataStore(StartpointManager.NAMESPACE, config, metricsRegistry);
    this.metadataStore.init();
    this.startpointManager = new StartpointManager(metadataStore);
    this.startpointManager.start();
  }

  public StartpointManager getStartpointManager() {
    return startpointManager;
  }

  public void stop() {
    startpointManager.stop();
    metadataStore.close();
  }
}
