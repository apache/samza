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

package org.apache.samza.system.eventhub.admin;

import com.microsoft.azure.eventhubs.EventHubPartitionRuntimeInformation;
import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.EventHubClientManager;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.consumer.EventHubSystemConsumer;
import org.apache.samza.system.eventhub.EventHubClientManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EventHubSystemAdmin implements SystemAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(EventHubSystemAdmin.class);
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = Duration.ofMinutes(1L).toMillis();

  private final EventHubClientManagerFactory eventHubClientManagerFactory;
  private final String systemName;
  private final EventHubConfig eventHubConfig;
  private final Map<String, EventHubClientManager> eventHubClients = new HashMap<>();
  private final Map<String, String[]> streamPartitions = new HashMap<>();

  public EventHubSystemAdmin(String systemName, EventHubConfig eventHubConfig,
                             EventHubClientManagerFactory eventHubClientManagerFactory) {
    this.systemName = systemName;
    this.eventHubConfig = eventHubConfig;
    this.eventHubClientManagerFactory = eventHubClientManagerFactory;
  }

  private String getNextOffset(String currentOffset) {
    // EventHub will return the first message AFTER the offset
    // that was specified in the fetch request.
    // If no such offset exists Eventhub will return an error.
    return String.valueOf(Long.parseLong(currentOffset) + 1);
  }

  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    Map<SystemStreamPartition, String> results = new HashMap<>();

    offsets.forEach((partition, offset) -> results.put(partition, getNextOffset(offset)));
    return results;
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    final Map<String, SystemStreamMetadata> requestedMetadata = new HashMap<>();
    final Map<String, CompletableFuture<EventHubRuntimeInformation>> ehRuntimeInfos = new HashMap<>();

    streamNames.forEach((streamName) -> {
        if (!streamPartitions.containsKey(streamName)) {
          EventHubClientManager eventHubClientManager = getOrCreateStreamEventHubClient(streamName);
          CompletableFuture<EventHubRuntimeInformation> runtimeInfo = eventHubClientManager.getEventHubClient()
                  .getRuntimeInformation();

          ehRuntimeInfos.put(streamName, runtimeInfo);
        }
      });

    try {
      ehRuntimeInfos.forEach((streamName, ehRuntimeInfo) -> {

          if (!streamPartitions.containsKey(streamName)) {
            LOG.debug(String.format("Partition ids for Stream=%s not found", streamName));
            try {

              long timeoutMs = eventHubConfig.getRuntimeInfoWaitTimeMS(systemName);
              EventHubRuntimeInformation ehInfo = ehRuntimeInfo.get(timeoutMs, TimeUnit.MILLISECONDS);

              LOG.debug(String.format("Adding partition ids for Stream=%s", streamName));
              streamPartitions.put(streamName, ehInfo.getPartitionIds());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {

              String msg = String.format("Error while fetching EventHubRuntimeInfo for System:%s, Stream:%s",
                      systemName, streamName);
              throw new SamzaException(msg);
            }
          }

          String[] partitionIds = streamPartitions.get(streamName);
          Map<Partition, SystemStreamPartitionMetadata> sspMetadataMap = getPartitionMetadata(streamName, partitionIds);
          SystemStreamMetadata systemStreamMetadata = new SystemStreamMetadata(streamName, sspMetadataMap);

          requestedMetadata.put(streamName, systemStreamMetadata);
        });

    } finally {

      // Closing clients
      eventHubClients.forEach((streamName, client) -> client.close(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
      eventHubClients.clear();
    }

    return requestedMetadata;
  }

  private EventHubClientManager getOrCreateStreamEventHubClient(String streamName) {
    if (!eventHubClients.containsKey(streamName)) {
      LOG.debug(String.format("Creating EventHubClient for Stream=%s", streamName));

      EventHubClientManager eventHubClientManager = eventHubClientManagerFactory
              .getEventHubClientManager(systemName, streamName, eventHubConfig);

      eventHubClientManager.init();
      eventHubClients.put(streamName, eventHubClientManager);
    }
    return eventHubClients.get(streamName);
  }

  private Map<Partition, SystemStreamPartitionMetadata> getPartitionMetadata(String streamName, String[] partitionIds) {
    EventHubClientManager eventHubClientManager = getOrCreateStreamEventHubClient(streamName);
    Map<Partition, SystemStreamPartitionMetadata> sspMetadataMap = new HashMap<>();
    Map<String, CompletableFuture<EventHubPartitionRuntimeInformation>> ehRuntimeInfos = new HashMap<>();

    for (String partition : partitionIds) {
      CompletableFuture<EventHubPartitionRuntimeInformation> partitionRuntimeInfo = eventHubClientManager
              .getEventHubClient()
              .getPartitionRuntimeInformation(partition);

      ehRuntimeInfos.put(partition, partitionRuntimeInfo);
    }

    ehRuntimeInfos.forEach((partitionId, ehPartitionRuntimeInfo) -> {
        try {
          long timeoutMs = eventHubConfig.getRuntimeInfoWaitTimeMS(systemName);
          EventHubPartitionRuntimeInformation ehPartitionInfo = ehPartitionRuntimeInfo.get(timeoutMs, TimeUnit.MILLISECONDS);

          // Set offsets
          String startingOffset = EventHubSystemConsumer.START_OF_STREAM;
          String newestOffset = ehPartitionInfo.getLastEnqueuedOffset();
          String upcomingOffset = getNextOffset(newestOffset);
          SystemStreamPartitionMetadata sspMetadata = new SystemStreamPartitionMetadata(startingOffset, newestOffset,
                  upcomingOffset);

          Partition partition = new Partition(Integer.parseInt(partitionId));

          sspMetadataMap.put(partition, sspMetadata);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          String msg = String.format(
                  "Error while fetching EventHubPartitionRuntimeInfo for System:%s, Stream:%s, Partition:%s",
                  systemName, streamName, partitionId);
          throw new SamzaException(msg);
        }
      });
    return sspMetadataMap;
  }

  public static Integer compareOffsets(String offset1, String offset2) {
    if (offset1 == null || offset2 == null) {
      return null;
    }
    // Should NOT be able to compare with END_OF_STREAM to allow new offsets to be
    // considered caught up if stream started at END_OF_STREAM offset
    if (EventHubSystemConsumer.END_OF_STREAM.equals(offset1) ||
            EventHubSystemConsumer.END_OF_STREAM.equals(offset2)) {
      return null;
    }
    try {
      return Long.compare(Long.parseLong(offset1), Long.parseLong(offset2));
    } catch (NumberFormatException exception) {
      return null;
    }
  }

  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    return compareOffsets(offset1, offset2);
  }
}
