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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.coordinator.stream.messages.SetExecutionContainerIdMapping;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.serializers.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for persisting and reading the execution environment container id information into the metadata store.
 * Processor id (logical Samza container id) to execution environment container id (ex: yarn container id) is written.
 **/
public class ExecutionContainerIdManager {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionContainerIdManager.class);

  private final Serde<String> valueSerde;
  private final MetadataStore metadataStore;

  /**
   * Builds the ExecutionContainerIdManager based upon the provided {@link MetadataStore} that is instantiated.
   * Uses the {@link CoordinatorStreamValueSerde} to serialize messages before reading/writing into metadata store.
   * @param metadataStore an instance of {@link MetadataStore} to read/write the container locality.
   */
  public ExecutionContainerIdManager(MetadataStore metadataStore) {
    this.metadataStore = metadataStore;
    this.valueSerde = new CoordinatorStreamValueSerde(SetExecutionContainerIdMapping.TYPE);
  }

  public void writeExecutionEnvironmentContainerIdMapping(String processorId, String executionEnvContainerId) {
    Preconditions.checkNotNull(processorId, "Container's logical processor id can not be null.");
    Preconditions.checkNotNull(executionEnvContainerId, "Container's physical execution environment container id can not be null.");
    LOG.info("Container {} has executionEnvContainerId as {}", processorId, executionEnvContainerId);
    metadataStore.put(processorId, valueSerde.toBytes(executionEnvContainerId));
    metadataStore.flush();
  }

  public Map<String, String> readExecutionEnvironmentContainerIdMapping() {
    Map<String, String> executionEnvironmentContainerIdMapping = new HashMap<>();
    metadataStore.all().forEach((processorId, valueBytes) -> {
      if (valueBytes != null) {
        String executionEnvContainerId = valueSerde.fromBytes(valueBytes);
        executionEnvironmentContainerIdMapping.put(processorId, executionEnvContainerId);
      }
    });
    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, String> entry : executionEnvironmentContainerIdMapping.entrySet()) {
        LOG.debug("Execution evironment container id for container {}: {}", entry.getKey(), entry.getValue());
      }
    }
    return executionEnvironmentContainerIdMapping;
  }

  public void close() {
    metadataStore.close();
  }
}
