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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.job.model.ProcessorLocality;
import org.apache.samza.job.model.LocalityModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.serializers.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for persisting and reading the locality information into the metadata store. Currently, we store the
 * processor-to-host assignment.
 * */
public class LocalityManager {
  private static final Logger LOG = LoggerFactory.getLogger(LocalityManager.class);

  private final Serde<String> valueSerde;
  private final MetadataStore metadataStore;

  /**
   * Builds the LocalityManager based upon the provided {@link MetadataStore} that is instantiated.
   * Setting up a metadata store instance is expensive which requires opening multiple connections
   * and reading tons of information. Fully instantiated metadata store is taken as a constructor argument
   * to reuse it across different utility classes. Uses the {@link CoordinatorStreamValueSerde} to serialize
   * messages before reading/writing into metadata store.
   *
   * @param metadataStore an instance of {@link MetadataStore} to read/write the container locality.
   */
  public LocalityManager(MetadataStore metadataStore) {
    this.metadataStore = metadataStore;
    this.valueSerde = new CoordinatorStreamValueSerde(SetContainerHostMapping.TYPE);
  }

  /**
   * Fetch the processor locality information from the {@link MetadataStore}. In YARN deployment model, the
   * processor refers to the samza container.
   *
   * @return the {@code LocalityModel} for the job
   */
  public LocalityModel readLocality() {
    Map<String, ProcessorLocality> containerLocalityMap = new HashMap<>();

    metadataStore.all().forEach((processorId, valueBytes) -> {
      if (valueBytes != null) {
        String locationId = valueSerde.fromBytes(valueBytes);
        containerLocalityMap.put(processorId, new ProcessorLocality(processorId, locationId));
      }
    });
    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, ProcessorLocality> entry : containerLocalityMap.entrySet()) {
        LOG.debug(String.format("Locality for container %s: %s", entry.getKey(), entry.getValue().host()));
      }
    }

    return new LocalityModel(containerLocalityMap);
  }

  /**
   * Method to write locality information to the {@link MetadataStore}. This method is used in {@link SamzaContainer}.
   *
   * @param processorId a.k.a logical container ID
   * @param hostName  the hostname
   */
  public void writeContainerToHostMapping(String processorId, String hostName) {
    String existingHostMapping = Optional.ofNullable(readLocality().getProcessorLocality(processorId))
        .map(ProcessorLocality::host)
        .orElse(null);
    if (StringUtils.isNotBlank(existingHostMapping) && !existingHostMapping.equals(hostName)) {
      LOG.info("Container {} moved from {} to {}", processorId, existingHostMapping, hostName);
    } else {
      LOG.info("Container {} started at {}", processorId, hostName);
    }

    metadataStore.put(processorId, valueSerde.toBytes(hostName));
    metadataStore.flush();
  }

  public void close() {
    metadataStore.close();
  }
}
