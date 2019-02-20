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

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for persisting and reading the container-to-host assignment information into the metadata store.
 * */
public class LocalityManager {
  private static final Logger LOG = LoggerFactory.getLogger(LocalityManager.class);

  private final Serde<String> valueSerde;
  private final MetadataStore metadataStore;

  /**
   * Builds the LocalityManager based upon {@link Config} and {@link MetricsRegistry}.
   * Uses the {@link CoordinatorStreamValueSerde} to serialize messages before
   * reading/writing into metadata store.
   *
   * @param config the configuration required for setting up metadata store.
   * @param metricsRegistry the registry for reporting metrics.
   */
  public LocalityManager(Config config, MetricsRegistry metricsRegistry) {
    this(config, metricsRegistry, new CoordinatorStreamValueSerde(SetContainerHostMapping.TYPE));
  }

  /**
   * Builds the LocalityManager based upon {@link Config} and {@link MetricsRegistry}.
   * Uses keySerde, valueSerde to serialize/deserialize (key, value) pairs before reading/writing
   * into {@link MetadataStore}.
   *
   * Key and value serializer are different for yarn (uses CoordinatorStreamMessage) and standalone (native ObjectOutputStream for serialization) modes.
   * @param config the configuration required for setting up metadata store.
   * @param metricsRegistry the registry for reporting metrics.
   * @param valueSerde the value serializer.
   */
  LocalityManager(Config config, MetricsRegistry metricsRegistry, Serde<String> valueSerde) {
    MetadataStoreFactory metadataStoreFactory = Util.getObj(new JobConfig(config).getMetadataStoreFactory(), MetadataStoreFactory.class);
    this.metadataStore = metadataStoreFactory.getMetadataStore(SetContainerHostMapping.TYPE, config, metricsRegistry);
    this.metadataStore.init();
    this.valueSerde = valueSerde;
  }

  /**
   * Method to allow read container locality information from the {@link MetadataStore}.
   * This method is used in {@link org.apache.samza.coordinator.JobModelManager}.
   *
   * @return the map of containerId: (hostname)
   */
  public Map<String, Map<String, String>> readContainerLocality() {
    Map<String, Map<String, String>> allMappings = new HashMap<>();
    metadataStore.all().forEach((containerId, valueBytes) -> {
        if (valueBytes != null) {
          String locationId = valueSerde.fromBytes(valueBytes);
          allMappings.put(containerId, ImmutableMap.of(SetContainerHostMapping.HOST_KEY, locationId));
        }
      });
    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, Map<String, String>> entry : allMappings.entrySet()) {
        LOG.debug(String.format("Locality for container %s: %s", entry.getKey(), entry.getValue()));
      }
    }

    return Collections.unmodifiableMap(allMappings);
  }

  /**
   * Method to write locality information to the {@link MetadataStore}. This method is used in {@link SamzaContainer}.
   *
   * @param containerId  the {@link SamzaContainer} ID
   * @param hostName  the hostname
   */
  public void writeContainerToHostMapping(String containerId, String hostName) {
    Map<String, Map<String, String>> containerToHostMapping = readContainerLocality();
    Map<String, String> existingMappings = containerToHostMapping.get(containerId);
    String existingHostMapping = existingMappings != null ? existingMappings.get(SetContainerHostMapping.HOST_KEY) : null;
    if (existingHostMapping != null && !existingHostMapping.equals(hostName)) {
      LOG.info("Container {} moved from {} to {}", containerId, existingHostMapping, hostName);
    } else {
      LOG.info("Container {} started at {}", containerId, hostName);
    }

    metadataStore.put(containerId, valueSerde.toBytes(hostName));
  }

  public void close() {
    metadataStore.close();
  }
}
