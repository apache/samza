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
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.coordinator.stream.CoordinatorStreamKeySerde;
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
 * Locality Manager is used to persist and read the container-to-host
 * assignment information from the coordinator stream.
 * */
public class LocalityManager {
  private static final Logger LOG = LoggerFactory.getLogger(LocalityManager.class);

  private final Config config;
  private Map<String, Map<String, String>> containerToHostMapping = new HashMap<>();
  private final Serde<String> keySerde;
  private final MetadataStore metadataStore;
  private final TaskAssignmentManager taskAssignmentManager;
  private final Serde<String> valueSerde;

  /**
   * Builds the LocalityManager based upon {@link Config} and {@link MetricsRegistry}.
   * Uses {@link CoordinatorStreamKeySerde} and {@link CoordinatorStreamValueSerde} to
   * serialize messages before reading/writing into coordinator stream.
   *
   * @param config denotes the configuration required for setting up metadata store.
   * @param metricsRegistry the registry for reporting metrics in metadata store.
   */
  public LocalityManager(Config config, MetricsRegistry metricsRegistry) {
    this(config, metricsRegistry, new CoordinatorStreamKeySerde(SetContainerHostMapping.TYPE),
         new CoordinatorStreamValueSerde(config, SetContainerHostMapping.TYPE));
  }

  /**
   * Builds the LocalityManager based upon {@link Config} and {@link MetricsRegistry}.
   * Uses keySerde, valueSerde to serialize/deserialize (key, value) pairs before reading/writing
   * into metadata store.
   *
   * Key and value serializer are different for yarn(uses CoordinatorStreamMessage) and standalone(native
   * ObjectOutputStream for serialization) modes.
   * @param config denotes the configuration required for setting up metadata store.
   * @param metricsRegistry the registry for reporting metrics in metadata store.
   * @param keySerde denotes the key serializer.
   * @param valueSerde denotes the value serializer.
   */
  public LocalityManager(Config config, MetricsRegistry metricsRegistry, Serde<String> keySerde, Serde<String> valueSerde) {
    this.config = config;
    MetadataStoreFactory metadataStoreFactory = Util.getObj(new JobConfig(config).getMetadataStoreFactory(), MetadataStoreFactory.class);
    this.metadataStore = metadataStoreFactory.getMetadataStore(SetContainerHostMapping.TYPE, config, metricsRegistry);
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.taskAssignmentManager = new TaskAssignmentManager(config, metricsRegistry, keySerde, valueSerde);
  }

  public void init(SamzaContainerContext containerContext) {
    this.metadataStore.init(containerContext);
    this.taskAssignmentManager.init(containerContext);
  }

  /**
   * Method to allow read container locality information from coordinator stream. This method is used
   * in {@link org.apache.samza.coordinator.JobModelManager}.
   *
   * @return the map of containerId: (hostname, jmxAddress, jmxTunnelAddress)
   */
  public Map<String, Map<String, String>> readContainerLocality() {
    Map<String, Map<String, String>> allMappings = new HashMap<>();
    metadataStore.all().forEach((keyBytes, valueBytes) -> {
        if (valueBytes != null) {
          String locationId = valueSerde.fromBytes(valueBytes);
          allMappings.put(keySerde.fromBytes(keyBytes), ImmutableMap.of(SetContainerHostMapping.HOST_KEY, locationId,
                                                                        SetContainerHostMapping.JMX_TUNNELING_URL_KEY, "",
                                                                        SetContainerHostMapping.JMX_URL_KEY, ""));
        }
      });
    containerToHostMapping = allMappings;
    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, Map<String, String>> entry : containerToHostMapping.entrySet()) {
        LOG.debug(String.format("Locality for container %s: %s", entry.getKey(), entry.getValue()));
      }
    }

    return Collections.unmodifiableMap(allMappings);
  }

  /**
   * Method to write locality info to coordinator stream. This method is used in {@link SamzaContainer}.
   *
   * @param containerId  the {@link SamzaContainer} ID
   * @param hostName  the hostname
   * @param jmxAddress  the JMX URL address
   * @param jmxTunnelingAddress  the JMX Tunnel URL address
   */
  public void writeContainerToHostMapping(String containerId, String hostName, String jmxAddress, String jmxTunnelingAddress) {
    Map<String, String> existingMappings = containerToHostMapping.get(containerId);
    String existingHostMapping = existingMappings != null ? existingMappings.get(SetContainerHostMapping.HOST_KEY) : null;
    if (existingHostMapping != null && !existingHostMapping.equals(hostName)) {
      LOG.info("Container {} moved from {} to {}", new Object[]{containerId, existingHostMapping, hostName});
    } else {
      LOG.info("Container {} started at {}", containerId, hostName);
    }

    metadataStore.put(keySerde.toBytes(containerId), valueSerde.toBytes(hostName));

    Map<String, String> mappings = new HashMap<>();
    mappings.put(SetContainerHostMapping.HOST_KEY, hostName);
    mappings.put(SetContainerHostMapping.JMX_URL_KEY, jmxAddress);
    mappings.put(SetContainerHostMapping.JMX_TUNNELING_URL_KEY, jmxTunnelingAddress);
    containerToHostMapping.put(containerId, mappings);
  }

  public void close() {
    metadataStore.close();
    taskAssignmentManager.close();
  }

  public TaskAssignmentManager getTaskAssignmentManager() {
    return taskAssignmentManager;
  }
}
