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
package org.apache.samza.diagnostics;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Defines the contents for any message emitted to the diagnostic stream by the {@link DiagnosticsManager}.
 * All contents are stored in a {@link MetricsHeader} and a metricsMessage map which combine to get a {@link MetricsSnapshot},
 * which can be serialized using serdes ({@link org.apache.samza.serializers.MetricsSnapshotSerdeV2}).
 * This class serializes {@link ContainerModel} using {@link SamzaObjectMapper} before adding to the metrics message.
 *
 */
public class DiagnosticsStreamMessage {
  private static final Logger LOG = LoggerFactory.getLogger(DiagnosticsStreamMessage.class);

  public static final String GROUP_NAME_FOR_DIAGNOSTICS_MANAGER = DiagnosticsManager.class.getName();
  // Using DiagnosticsManager as the group name for processor-stop-events, job-related params, and container model

  private static final String SAMZACONTAINER_METRICS_GROUP_NAME = "org.apache.samza.container.SamzaContainerMetrics";
  // Using SamzaContainerMetrics as the group name for exceptions to maintain compatibility with existing diagnostics
  private static final String EXCEPTION_LIST_METRIC_NAME = "exceptions";

  private static final String STOP_EVENT_LIST_METRIC_NAME = "stopEvents";
  private static final String CONTAINER_MB_METRIC_NAME = "containerMemoryMb";
  private static final String CONTAINER_NUM_CORES_METRIC_NAME = "containerNumCores";
  private static final String CONTAINER_NUM_PERSISTENT_STORES_METRIC_NAME = "numPersistentStores";
  private static final String CONTAINER_MAX_CONFIGURED_HEAP_METRIC_NAME = "maxHeap";
  private static final String CONTAINER_THREAD_POOL_SIZE_METRIC_NAME = "containerThreadPoolSize";
  private static final String CONTAINER_MODELS_METRIC_NAME = "containerModels";
  private static final String AUTOSIZING_ENABLED_METRIC_NAME = "autosizingEnabled";
  private static final String CONFIG_METRIC_NAME = "config";

  private final MetricsHeader metricsHeader;
  private final Map<String, Map<String, Object>> metricsMessage;

  public DiagnosticsStreamMessage(String jobName, String jobId, String containerName, String executionEnvContainerId,
      String taskClassVersion, String samzaVersion, String hostname, long timestamp, long resetTimestamp) {

    // Create the metricHeader
    metricsHeader =
        new MetricsHeader(jobName, jobId, containerName, executionEnvContainerId, DiagnosticsManager.class.getName(),
            taskClassVersion, samzaVersion, hostname, timestamp, resetTimestamp);

    this.metricsMessage = new HashMap<>();
  }

  /**
   * Add the container memory mb parameter to the message.
   * @param containerMemoryMb the memory mb parameter value.
   */
  public void addContainerMb(Integer containerMemoryMb) {
    addToMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONTAINER_MB_METRIC_NAME, containerMemoryMb);
  }

  /**
   * Add the container num cores parameter to the message.
   * @param containerNumCores the num core parameter value.
   */
  public void addContainerNumCores(Integer containerNumCores) {
    addToMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONTAINER_NUM_CORES_METRIC_NAME, containerNumCores);
  }

  /**
   * Add the num stores with changelog parameter to the message.
   * @param numPersistentStores the parameter value.
   */
  public void addNumPersistentStores(Integer numPersistentStores) {
    addToMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONTAINER_NUM_PERSISTENT_STORES_METRIC_NAME,
        numPersistentStores);
  }

  /**
   * Add the configured max heap size in bytes.
   * @param maxHeapSize the parameter value.
   */
  public void addMaxHeapSize(Long maxHeapSize) {
    addToMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONTAINER_MAX_CONFIGURED_HEAP_METRIC_NAME, maxHeapSize);
  }

  /**
   * Add the configured container thread pool size.
   * @param threadPoolSize the parameter value.
   */
  public void addContainerThreadPoolSize(Integer threadPoolSize) {
    addToMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONTAINER_THREAD_POOL_SIZE_METRIC_NAME, threadPoolSize);
  }

  /**
   * Add a map of container models (indexed by containerID) to the message.
   * @param containerModelMap the container models map
   */
  public void addContainerModels(Map<String, ContainerModel> containerModelMap) {
    if (containerModelMap != null && !containerModelMap.isEmpty()) {
      addToMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONTAINER_MODELS_METRIC_NAME,
          serializeContainerModelMap(containerModelMap));
    }
  }

  /**
   * Add the current auto-scaling setting.
   * @param autosizingEnabled the parameter value.
   */
  public void addAutosizingEnabled(Boolean autosizingEnabled) {
    addToMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, AUTOSIZING_ENABLED_METRIC_NAME, autosizingEnabled);
  }

  /**
   * Add a list of {@link DiagnosticsExceptionEvent}s to the message.
   * @param exceptionList the list to add.
   */
  public void addDiagnosticsExceptionEvents(Collection<DiagnosticsExceptionEvent> exceptionList) {
    if (exceptionList != null && !exceptionList.isEmpty()) {
      addToMetricsMessage(SAMZACONTAINER_METRICS_GROUP_NAME, EXCEPTION_LIST_METRIC_NAME, exceptionList);
    }
  }

  /**
   * Add a list of {@link org.apache.samza.diagnostics.ProcessorStopEvent}s to add to the list.
   * @param stopEventList the list to add.
   */
  public void addProcessorStopEvents(List<ProcessorStopEvent> stopEventList) {
    if (stopEventList != null && !stopEventList.isEmpty()) {
      addToMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, STOP_EVENT_LIST_METRIC_NAME, stopEventList);
    }
  }

  /**
   * Add the job's config to the message.
   * @param config the config to add.
   */
  public void addConfig(Config config) {
    addToMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONFIG_METRIC_NAME, (Map<String, String>) config);
  }

  /**
   * Convert this message into a {@link MetricsSnapshot}, useful for serde-deserde using {@link org.apache.samza.serializers.MetricsSnapshotSerde}.
   * @return
   */
  public MetricsSnapshot convertToMetricsSnapshot() {
    MetricsSnapshot metricsSnapshot = new MetricsSnapshot(metricsHeader, new Metrics(metricsMessage));
    return metricsSnapshot;
  }

  /**
   * Check if the message has no contents.
   * @return True if the message is empty, false otherwise.
   */
  public boolean isEmpty() {
    return metricsMessage.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DiagnosticsStreamMessage that = (DiagnosticsStreamMessage) o;
    return metricsHeader.getAsMap().equals(that.metricsHeader.getAsMap()) && metricsMessage.equals(that.metricsMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricsHeader, metricsMessage);
  }

  public Collection<ProcessorStopEvent> getProcessorStopEvents() {
    return (Collection<ProcessorStopEvent>) getFromMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER,
        STOP_EVENT_LIST_METRIC_NAME);
  }

  public Collection<DiagnosticsExceptionEvent> getExceptionEvents() {
    return (Collection<DiagnosticsExceptionEvent>) getFromMetricsMessage(SAMZACONTAINER_METRICS_GROUP_NAME,
        EXCEPTION_LIST_METRIC_NAME);
  }

  public Integer getContainerMb() {
    return (Integer) getFromMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONTAINER_MB_METRIC_NAME);
  }

  public Integer getContainerNumCores() {
    return (Integer) getFromMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONTAINER_NUM_CORES_METRIC_NAME);
  }

  public Integer getNumPersistentStores() {
    return (Integer) getFromMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER,
        CONTAINER_NUM_PERSISTENT_STORES_METRIC_NAME);
  }

  public Long getMaxHeapSize() {
    return (Long) getFromMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONTAINER_MAX_CONFIGURED_HEAP_METRIC_NAME);
  }

  public Integer getContainerThreadPoolSize() {
    return (Integer) getFromMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONTAINER_THREAD_POOL_SIZE_METRIC_NAME);
  }

  public Map<String, ContainerModel> getContainerModels() {
    return deserializeContainerModelMap((String) getFromMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONTAINER_MODELS_METRIC_NAME));
  }

  public Boolean getAutosizingEnabled() {
    return (Boolean) getFromMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, AUTOSIZING_ENABLED_METRIC_NAME);
  }

  /**
   * This method gets the config of the job from the MetricsMessage.
   * @return the config of the job.
   */
  public Config getConfig() {
    return new MapConfig((Map<String, String>) getFromMetricsMessage(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, CONFIG_METRIC_NAME));
  }

  // Helper method to get a {@link DiagnosticsStreamMessage} from a {@link MetricsSnapshot}.
  //   * This is typically used when deserializing messages from a diagnostics-stream.
  //   * @param metricsSnapshot
  public static DiagnosticsStreamMessage convertToDiagnosticsStreamMessage(MetricsSnapshot metricsSnapshot) {
    DiagnosticsStreamMessage diagnosticsStreamMessage =
        new DiagnosticsStreamMessage(metricsSnapshot.getHeader().getJobName(), metricsSnapshot.getHeader().getJobId(),
            metricsSnapshot.getHeader().getContainerName(), metricsSnapshot.getHeader().getExecEnvironmentContainerId(),
            metricsSnapshot.getHeader().getVersion(), metricsSnapshot.getHeader().getSamzaVersion(),
            metricsSnapshot.getHeader().getHost(), metricsSnapshot.getHeader().getTime(),
            metricsSnapshot.getHeader().getResetTime());

    Map<String, Map<String, Object>> metricsMap = metricsSnapshot.getMetrics().getAsMap();
    Map<String, Object> diagnosticsManagerGroupMap = metricsMap.get(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER);
    Map<String, Object> containerMetricsGroupMap = metricsMap.get(SAMZACONTAINER_METRICS_GROUP_NAME);

    if (diagnosticsManagerGroupMap != null) {

      diagnosticsStreamMessage.addContainerNumCores((Integer) diagnosticsManagerGroupMap.get(CONTAINER_NUM_CORES_METRIC_NAME));
      diagnosticsStreamMessage.addContainerMb((Integer) diagnosticsManagerGroupMap.get(CONTAINER_MB_METRIC_NAME));
      diagnosticsStreamMessage.addNumPersistentStores((Integer) diagnosticsManagerGroupMap.get(
          CONTAINER_NUM_PERSISTENT_STORES_METRIC_NAME));
      diagnosticsStreamMessage.addContainerModels(deserializeContainerModelMap((String) diagnosticsManagerGroupMap.get(CONTAINER_MODELS_METRIC_NAME)));
      diagnosticsStreamMessage.addMaxHeapSize((Long) diagnosticsManagerGroupMap.get(CONTAINER_MAX_CONFIGURED_HEAP_METRIC_NAME));
      diagnosticsStreamMessage.addContainerThreadPoolSize((Integer) diagnosticsManagerGroupMap.get(CONTAINER_THREAD_POOL_SIZE_METRIC_NAME));
      diagnosticsStreamMessage.addProcessorStopEvents((List<ProcessorStopEvent>) diagnosticsManagerGroupMap.get(STOP_EVENT_LIST_METRIC_NAME));
      diagnosticsStreamMessage.addAutosizingEnabled((Boolean) diagnosticsManagerGroupMap.get(AUTOSIZING_ENABLED_METRIC_NAME));
      diagnosticsStreamMessage.addConfig(new MapConfig((Map<String, String>) diagnosticsManagerGroupMap.get(CONFIG_METRIC_NAME)));
    }

    if (containerMetricsGroupMap != null && containerMetricsGroupMap.containsKey(EXCEPTION_LIST_METRIC_NAME)) {
      diagnosticsStreamMessage.addDiagnosticsExceptionEvents(
          (Collection<DiagnosticsExceptionEvent>) containerMetricsGroupMap.get(EXCEPTION_LIST_METRIC_NAME));
    }

    return diagnosticsStreamMessage;
  }

  /**
   * Helper method to use {@link SamzaObjectMapper} to serialize {@link ContainerModel}s.
   * We use SamzaObjectMapper for ContainerModels, rather than using ObjectMapper (in MetricsSnapshotSerdeV2)
   * because MetricsSnapshotSerdeV2 enables default typing, which writes type information for all containerModel (and
   * underlying) classes, deserializing which requires a large number of jackson related changes to those classes
   * (annotations and/or mixins). We cannot disable default typing to avoid backward incompatibility. This is why
   * we serde-deserde ContainerModel explicitly using SamzaObjectMapper (which is also used for reads-writes to coordinator
   * stream).
   * {@link SamzaObjectMapper} provides several conventions and optimizations for serializing containerModels.
   * @param containerModelMap map of container models to serialize.
   * @return
   */
  private static String serializeContainerModelMap(Map<String, ContainerModel> containerModelMap) {
    ObjectMapper samzaObjectMapper = SamzaObjectMapper.getObjectMapper();
    try {
      return samzaObjectMapper.writeValueAsString(containerModelMap);
    } catch (IOException e) {
      LOG.error("Exception in serializing container model ", e);
    }

    return null;
  }

  /**
   * Helper method to use {@link SamzaObjectMapper} to deserialize {@link ContainerModel}s.
   * {@link SamzaObjectMapper} provides several conventions and optimizations for deserializing containerModels.
   * @return
   */
  private static Map<String, ContainerModel> deserializeContainerModelMap(
      String serializedContainerModel) {
    Map<String, ContainerModel> containerModelMap = null;
    ObjectMapper samzaObjectMapper = SamzaObjectMapper.getObjectMapper();

    try {
      if (serializedContainerModel != null) {
        containerModelMap = samzaObjectMapper.readValue(serializedContainerModel, new TypeReference<Map<String, ContainerModel>>() {
        });
      }
    } catch (IOException e) {
      LOG.error("Exception in deserializing container model ", e);
    }

    return containerModelMap;
  }

  private void addToMetricsMessage(String groupName, String metricName, Object value) {
    if (value != null) {
      metricsMessage.putIfAbsent(groupName, new HashMap<>());
      metricsMessage.get(groupName).put(metricName, value);
    }
  }

  private Object getFromMetricsMessage(String groupName, String metricName) {
    if (metricsMessage.containsKey(groupName) && metricsMessage.get(groupName) != null) {
      return metricsMessage.get(groupName).get(metricName);
    } else {
      return null;
    }
  }
}
