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
import org.apache.samza.system.eventhub.EventHubClientFactory;
import org.apache.samza.system.eventhub.EventHubClientWrapper;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.consumer.EventHubSystemConsumer;
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
  private final EventHubClientFactory eventHubClientFactory = new EventHubClientFactory();
  private String systemName;
  private EventHubConfig eventHubConfig;
  private Map<String, EventHubClientWrapper> eventHubClients = new HashMap<>();
  private Map<String, String[]> streamPartitions = new HashMap<>();

  public EventHubSystemAdmin(String systemName, EventHubConfig config) {
    this.systemName = systemName;
    eventHubConfig = config;
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
          EventHubClientWrapper eventHubClientWrapper = getStreamEventHubClient(streamName);
          ehRuntimeInfos.put(streamName,
                  eventHubClientWrapper.getEventHubClient().getRuntimeInformation());
        }
      });
    ehRuntimeInfos.forEach((streamName, ehRuntimeInfo) -> {
        if (!streamPartitions.containsKey(streamName)) {
          try {
            EventHubRuntimeInformation ehInfo = ehRuntimeInfo.get(eventHubConfig.getRuntimeInfoWaitTimeMS(),
                    TimeUnit.MILLISECONDS);
            streamPartitions.put(streamName, ehInfo.getPartitionIds());
          } catch (InterruptedException | ExecutionException e) {
            String msg = String.format("Error while fetching EventHubRuntimeInfo for System:%s, Stream:%s",
                    systemName, streamName);
            LOG.error(msg, e);
            throw new SamzaException(msg);
          } catch (TimeoutException e) {
            String msg = String.format("Timed out while fetching EventHubRuntimeInfo for System:%s, Stream:%s",
                    systemName, streamName);
            LOG.error(msg, e);
            throw new SamzaException(msg);
          }
        }
        requestedMetadata.put(streamName, new SystemStreamMetadata(streamName,
                getPartitionMetadata(streamName, streamPartitions.get(streamName))));
      });
    return requestedMetadata;
  }

  private EventHubClientWrapper getStreamEventHubClient(String streamName) {
    if (!eventHubClients.containsKey(streamName)) {
      eventHubClients.put(streamName, eventHubClientFactory
              .getEventHubClient(eventHubConfig.getStreamNamespace(streamName),
                      eventHubConfig.getStreamEntityPath(streamName), eventHubConfig.getStreamSasKeyName(streamName),
                      eventHubConfig.getStreamSasToken(streamName), eventHubConfig));
      eventHubClients.get(streamName).init();
    }
    return eventHubClients.get(streamName);
  }

  private Map<Partition, SystemStreamPartitionMetadata> getPartitionMetadata(String streamName, String[] partitionIds) {
    EventHubClientWrapper eventHubClientWrapper = getStreamEventHubClient(streamName);
    Map<Partition, SystemStreamPartitionMetadata> sspMetadataMap = new HashMap<>();
    Map<String, CompletableFuture<EventHubPartitionRuntimeInformation>> ehRuntimeInfos = new HashMap<>();
    for (String partition : partitionIds) {
      ehRuntimeInfos.put(partition, eventHubClientWrapper.getEventHubClient()
               .getPartitionRuntimeInformation(partition));
    }
    ehRuntimeInfos.forEach((partitionId, ehPartitionRuntimeInfo) -> {
        try {
          EventHubPartitionRuntimeInformation ehPartitionInfo = ehPartitionRuntimeInfo
                  .get(eventHubConfig.getRuntimeInfoWaitTimeMS(), TimeUnit.MILLISECONDS);
          sspMetadataMap.put(new Partition(Integer.parseInt(partitionId)),
                  new SystemStreamPartitionMetadata(EventHubSystemConsumer.START_OF_STREAM,
                  ehPartitionInfo.getLastEnqueuedOffset(), EventHubSystemConsumer.END_OF_STREAM));
        } catch (InterruptedException | ExecutionException e) {
          String msg = String.format(
                  "Error while fetching EventHubPartitionRuntimeInfo for System:%s, Stream:%s, Partition:%s",
                  systemName, streamName, partitionId);
          LOG.error(msg, e);
          throw new SamzaException(msg);
        } catch (TimeoutException e) {
          String msg = String.format(
                  "Timed out while fetching EventHubRuntimeInfo for System:%s, Stream:%s, , Partition:%s",
                  systemName, streamName, partitionId);
          LOG.error(msg, e);
          throw new SamzaException(msg);
        }
      });
    return sspMetadataMap;
  }

  @Override
  public void createChangelogStream(String streamName, int numOfPartitions) {
    throw new UnsupportedOperationException("Event Hubs does not support change log stream.");
  }

  @Override
  public void validateChangelogStream(String streamName, int numOfPartitions) {
    throw new UnsupportedOperationException("Event Hubs does not support change log stream.");
  }

  @Override
  public void createCoordinatorStream(String streamName) {
    throw new UnsupportedOperationException("Event Hubs does not support coordinator stream.");
  }

  @Override
  public Integer offsetComparator(String offset1, String offset2) {
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
