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

package org.apache.samza.coordinator.metadatastore;

import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.NoOpMetricsRegistry;

/**
 * <p>
 * Instantiates and initializes a {@link CoordinatorStreamStore} based upon the provided configuration.
 *
 * Primarily used for testing the metadata read/write flows in ApplicationMaster.
 * </p>
 */
public class CoordinatorStreamStoreTestUtil {

  private final CoordinatorStreamStore coordinatorStreamStore;
  private final MockCoordinatorStreamSystemFactory systemFactory;
  private final Config config;

  public CoordinatorStreamStoreTestUtil(Config config) {
    this(config, "test-kafka");
  }

  public CoordinatorStreamStoreTestUtil(Config config, String systemName) {
    this.config = config;
    this.systemFactory = new MockCoordinatorStreamSystemFactory();
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache();
    SystemConsumer systemConsumer = systemFactory.getConsumer(systemName, config, new NoOpMetricsRegistry());
    SystemProducer systemProducer = systemFactory.getProducer(systemName, config, new NoOpMetricsRegistry());
    SystemAdmin systemAdmin = systemFactory.getAdmin(systemName, config);
    this.coordinatorStreamStore = new CoordinatorStreamStore(config, systemProducer, systemConsumer, systemAdmin);
    this.coordinatorStreamStore.init();
  }

  public CoordinatorStreamStore getCoordinatorStreamStore() {
    return coordinatorStreamStore;
  }

  public MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemProducer getMockCoordinatorStreamSystemProducer() {
    return systemFactory.getCoordinatorStreamSystemProducer(config, null);
  }

  public MockCoordinatorStreamSystemFactory.MockCoordinatorStreamSystemConsumer getMockCoordinatorStreamSystemConsumer() {
    return systemFactory.getCoordinatorStreamSystemConsumer(config, null);
  }
}
