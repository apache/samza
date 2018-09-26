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

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import com.microsoft.azure.eventhubs.PartitionRuntimeInformation;

import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.List;
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

  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    // In EventHubSystemConsumer#initializeEventHubsManagers, we exclude the offset that we specify. i.e.
    // we will only get the message after the checkpoint offset. Hence, by returning the same offset as the
    // "next" offset, we won't be reprocessing the same event.
    return offsets;
  }

  // EventHubRuntimeInformation does not implement toString()
  private String printEventHubRuntimeInfo(EventHubRuntimeInformation ehInfo) {
    if (ehInfo == null) {
      return "[EventHubRuntimeInformation: null]";
    }
    return String.format("[EventHubRuntimeInformation: createAt=%s, partitionCount=%d, path=%s]", ehInfo.getCreatedAt(),
        ehInfo.getPartitionCount(), ehInfo.getPath());
  }

  // PartitionRuntimeInformation does not implement toString()
  private String printPartitionRuntimeInfo(PartitionRuntimeInformation runtimeInformation) {
    if (runtimeInformation == null) {
      return "[PartitionRuntimeInformation: null]";
    }
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("[PartitionRuntimeInformation:");
    stringBuilder.append(" eventHubPath=").append(runtimeInformation.getEventHubPath());
    stringBuilder.append(" partitionId=").append(runtimeInformation.getPartitionId());
    stringBuilder.append(" lastEnqueuedTimeUtc=").append(runtimeInformation.getLastEnqueuedTimeUtc().toString());
    stringBuilder.append(" lastEnqueuedOffset=").append(runtimeInformation.getLastEnqueuedOffset());
    // calculate the number of messages in the queue
    stringBuilder.append(" numMessages=")
        .append(runtimeInformation.getLastEnqueuedSequenceNumber() - runtimeInformation.getBeginSequenceNumber());
    stringBuilder.append("]");
    return stringBuilder.toString();
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    Map<String, SystemStreamMetadata> requestedMetadata = new HashMap<>();

    try {
      for (String streamName : streamNames) {
        if (!streamPartitions.containsKey(streamName)) {
          LOG.debug(String.format("Partition ids for Stream=%s not found", streamName));

          EventHubClientManager eventHubClientManager = getOrCreateStreamEventHubClient(streamName);
          EventHubClient ehClient = eventHubClientManager.getEventHubClient();

          CompletableFuture<EventHubRuntimeInformation> runtimeInfo = ehClient.getRuntimeInformation();
          long timeoutMs = eventHubConfig.getRuntimeInfoWaitTimeMS(systemName);
          EventHubRuntimeInformation ehInfo = runtimeInfo.get(timeoutMs, TimeUnit.MILLISECONDS);
          LOG.info(String.format("Adding partition ids=%s for stream=%s. EHRuntimetInfo=%s",
              Arrays.toString(ehInfo.getPartitionIds()), streamName, printEventHubRuntimeInfo(ehInfo)));
          streamPartitions.put(streamName, ehInfo.getPartitionIds());
        }
        String[] partitionIds = streamPartitions.get(streamName);
        Map<Partition, SystemStreamPartitionMetadata> sspMetadataMap = getPartitionMetadata(streamName, partitionIds);
        SystemStreamMetadata systemStreamMetadata = new SystemStreamMetadata(streamName, sspMetadataMap);
        requestedMetadata.put(streamName, systemStreamMetadata);
      }
    } catch (Exception e) {
      String msg = String.format("Error while fetching EventHubRuntimeInfo for System:%s", systemName);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    } finally {
      // Closing clients
      eventHubClients.forEach((streamName, client) -> client.close(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
      eventHubClients.clear();
    }

    return requestedMetadata;
  }

  private EventHubClientManager getOrCreateStreamEventHubClient(String streamName) {
    if (!eventHubClients.containsKey(streamName)) {
      LOG.info(String.format("Creating EventHubClient for Stream=%s", streamName));

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
    List<CompletableFuture<PartitionRuntimeInformation>> futureList = new ArrayList<>();

    for (String partition : partitionIds) {
      CompletableFuture<PartitionRuntimeInformation> partitionRuntimeInfo = eventHubClientManager
              .getEventHubClient()
              .getPartitionRuntimeInformation(partition);
      futureList.add(partitionRuntimeInfo);
      partitionRuntimeInfo.thenAccept(ehPartitionInfo -> {
          LOG.info(printPartitionRuntimeInfo(ehPartitionInfo));
          // Set offsets
          String startingOffset = EventHubSystemConsumer.START_OF_STREAM;
          String newestOffset = ehPartitionInfo.getLastEnqueuedOffset();
          String upcomingOffset = EventHubSystemConsumer.END_OF_STREAM;
          SystemStreamPartitionMetadata sspMetadata = new SystemStreamPartitionMetadata(startingOffset, newestOffset,
            upcomingOffset);
          sspMetadataMap.put(new Partition(Integer.parseInt(partition)), sspMetadata);
        });
    }

    CompletableFuture<Void> futureGroup =
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
    long timeoutMs = eventHubConfig.getRuntimeInfoWaitTimeMS(systemName);
    try {
      futureGroup.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      String msg = String.format(
          "Error while fetching EventHubPartitionRuntimeInfo for System:%s, Stream:%s",
          systemName, streamName);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    }
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
