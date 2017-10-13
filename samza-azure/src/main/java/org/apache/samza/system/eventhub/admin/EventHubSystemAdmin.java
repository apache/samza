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
import org.apache.samza.system.eventhub.SamzaEventHubClient;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.consumer.EventHubSystemConsumer;
import org.apache.samza.system.eventhub.SamzaEventHubClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EventHubSystemAdmin implements SystemAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(EventHubSystemAdmin.class);
  private final SamzaEventHubClientFactory samzaEventHubClientFactory;
  private String systemName;
  private EventHubConfig eventHubConfig;
  private Map<String, SamzaEventHubClient> eventHubClients = new HashMap<>();
  private Map<String, String[]> streamPartitions = new HashMap<>();

  public EventHubSystemAdmin(String systemName, EventHubConfig eventHubConfig,
                             SamzaEventHubClientFactory samzaEventHubClientFactory) {
    this.systemName = systemName;
    this.eventHubConfig = eventHubConfig;
    this.samzaEventHubClientFactory = samzaEventHubClientFactory;
  }

  private static String getNextOffset(String currentOffset) {
    // EventHub will return the first message AFTER the offset
    // that was specified in the fetch request.
    return currentOffset.equals(EventHubSystemConsumer.END_OF_STREAM) ? currentOffset :
            String.valueOf(Long.parseLong(currentOffset) + 1);
  }

  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    Map<SystemStreamPartition, String> results = new HashMap<>();

    offsets.forEach((partition, offset) -> results.put(partition, getNextOffset(offset)));
    return results;
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    Map<String, SystemStreamMetadata> requestedMetadata = new HashMap<>();
    Map<String, CompletableFuture<EventHubRuntimeInformation>> ehRuntimeInfos = new HashMap<>();

    streamNames.forEach((streamName) -> {
        if (!streamPartitions.containsKey(streamName)) {
          SamzaEventHubClient samzaEventHubClient = getOrCreateStreamEventHubClient(streamName);
          CompletableFuture<EventHubRuntimeInformation> runtimeInfo = samzaEventHubClient.getEventHubClient()
                  .getRuntimeInformation();

          ehRuntimeInfos.put(streamName, runtimeInfo);
        }
      });
    ehRuntimeInfos.forEach((streamName, ehRuntimeInfo) -> {
        if (!streamPartitions.containsKey(streamName)) {
          try {
            long timeoutMs = eventHubConfig.getRuntimeInfoWaitTimeMS(systemName);
            EventHubRuntimeInformation ehInfo = ehRuntimeInfo.get(timeoutMs, TimeUnit.MILLISECONDS);

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
    return requestedMetadata;
  }

  private SamzaEventHubClient getOrCreateStreamEventHubClient(String streamName) {
    if (!eventHubClients.containsKey(streamName)) {
      SamzaEventHubClient samzaEventHubClient = samzaEventHubClientFactory
              .getSamzaEventHubClient(systemName, streamName, eventHubConfig);

      samzaEventHubClient.init();
      eventHubClients.put(streamName, samzaEventHubClient);
    }
    return eventHubClients.get(streamName);
  }

  private Map<Partition, SystemStreamPartitionMetadata> getPartitionMetadata(String streamName, String[] partitionIds) {
    SamzaEventHubClient samzaEventHubClient = getOrCreateStreamEventHubClient(streamName);
    Map<Partition, SystemStreamPartitionMetadata> sspMetadataMap = new HashMap<>();
    Map<String, CompletableFuture<EventHubPartitionRuntimeInformation>> ehRuntimeInfos = new HashMap<>();

    for (String partition : partitionIds) {
      CompletableFuture<EventHubPartitionRuntimeInformation> partitionRuntimeInfo = samzaEventHubClient
              .getEventHubClient()
              .getPartitionRuntimeInformation(partition);

      ehRuntimeInfos.put(partition, partitionRuntimeInfo);
    }

    ehRuntimeInfos.forEach((partitionId, ehPartitionRuntimeInfo) -> {
        try {
          long timeoutMs = eventHubConfig.getRuntimeInfoWaitTimeMS(systemName);
          EventHubPartitionRuntimeInformation ehPartitionInfo = ehPartitionRuntimeInfo.get(timeoutMs, TimeUnit.MILLISECONDS);

          String startingOffset = EventHubSystemConsumer.START_OF_STREAM;
          String newestOffset = ehPartitionInfo.getLastEnqueuedOffset();
          String upcomingOffset = EventHubSystemConsumer.END_OF_STREAM;
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

  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    if (offset1 == null || offset2 == null) {
      return null;
    }
    try {
      if (offset1.equals(EventHubSystemConsumer.END_OF_STREAM)) {
        return offset2.equals(EventHubSystemConsumer.END_OF_STREAM) ? 0 : 1;
      }
      return offset2.equals(EventHubSystemConsumer.END_OF_STREAM) ? -1 :
              Long.compare(Long.parseLong(offset1), Long.parseLong(offset2));
    } catch (NumberFormatException exception) {
      return null;
    }
  }
}
