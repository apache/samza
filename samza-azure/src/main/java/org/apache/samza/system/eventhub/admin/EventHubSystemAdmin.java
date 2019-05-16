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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import com.microsoft.azure.eventhubs.EventPosition;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.PartitionRuntimeInformation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.startpoint.Startpoint;
import org.apache.samza.startpoint.StartpointOldest;
import org.apache.samza.startpoint.StartpointSpecific;
import org.apache.samza.startpoint.StartpointTimestamp;
import org.apache.samza.startpoint.StartpointUpcoming;
import org.apache.samza.startpoint.StartpointVisitor;
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
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = Duration.ofMinutes(1).toMillis();

  private final EventHubClientManagerFactory eventHubClientManagerFactory;
  private final String systemName;
  private final EventHubConfig eventHubConfig;
  private final Map<String, EventHubClientManager> eventHubClients = new HashMap<>();
  private final Map<String, String[]> streamPartitions = new HashMap<>();
  private final EventHubSamzaOffsetResolver eventHubSamzaOffsetResolver;

  public EventHubSystemAdmin(String systemName, EventHubConfig eventHubConfig,
                             EventHubClientManagerFactory eventHubClientManagerFactory) {
    this.systemName = systemName;
    this.eventHubConfig = eventHubConfig;
    this.eventHubClientManagerFactory = eventHubClientManagerFactory;
    this.eventHubSamzaOffsetResolver = new EventHubSamzaOffsetResolver(this, eventHubConfig);
  }

  @Override
  public void stop() {
    for (Map.Entry<String, EventHubClientManager> entry : eventHubClients.entrySet()) {
      EventHubClientManager eventHubClientManager = entry.getValue();
      try {
        eventHubClientManager.close(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
      } catch (Exception e) {
        LOG.warn(String.format("Exception occurred when closing EventHubClient of stream: %s.", entry.getKey()), e);
      }
    }
    eventHubClients.clear();
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
  private String printPartitionRuntimeInfo(PartitionRuntimeInformation runtimeInfo) {
    if (runtimeInfo == null) {
      return "[PartitionRuntimeInformation: null]";
    }
    // calculate the number of messages in the queue
    return "[PartitionRuntimeInformation:"
      + " eventHubPath=" + runtimeInfo.getEventHubPath()
      + " partitionId=" + runtimeInfo.getPartitionId()
      + " lastEnqueuedTimeUtc=" + runtimeInfo.getLastEnqueuedTimeUtc()
      + " lastEnqueuedOffset=" + runtimeInfo.getLastEnqueuedOffset()
      + " numMessages=" + (runtimeInfo.getLastEnqueuedSequenceNumber() - runtimeInfo.getBeginSequenceNumber())
      + "]";
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
      return requestedMetadata;
    } catch (Exception e) {
      String msg = String.format("Error while fetching EventHubRuntimeInfo for System:%s", systemName);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    }
  }

  @Override
  public String resolveStartpointToOffset(SystemStreamPartition systemStreamPartition, Startpoint startpoint) {
    return startpoint.apply(systemStreamPartition, eventHubSamzaOffsetResolver);
  }

  EventHubClientManager getOrCreateStreamEventHubClient(String streamName) {
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

  /**
   * Offers a eventhub specific implementation of {@link StartpointVisitor} that resolves
   * different types of {@link Startpoint} to samza offset.
   */
  @VisibleForTesting
  static class EventHubSamzaOffsetResolver implements StartpointVisitor<SystemStreamPartition, String> {

    private final EventHubSystemAdmin eventHubSystemAdmin;
    private final EventHubConfig eventHubConfig;

    EventHubSamzaOffsetResolver(EventHubSystemAdmin eventHubSystemAdmin, EventHubConfig eventHubConfig) {
      this.eventHubSystemAdmin = eventHubSystemAdmin;
      this.eventHubConfig = eventHubConfig;
    }

    @Override
    public String visit(SystemStreamPartition systemStreamPartition, StartpointSpecific startpointSpecific) {
      return startpointSpecific.getSpecificOffset();
    }

    @Override
    public String visit(SystemStreamPartition systemStreamPartition, StartpointTimestamp startpointTimestamp) {
      String streamName = systemStreamPartition.getStream();
      EventHubClientManager eventHubClientManager = eventHubSystemAdmin.getOrCreateStreamEventHubClient(streamName);
      EventHubClient eventHubClient = eventHubClientManager.getEventHubClient();

      PartitionReceiver partitionReceiver = null;
      try {
        // 1. Initialize the arguments required for creating the partition receiver.
        String partitionId = String.valueOf(systemStreamPartition.getPartition().getPartitionId());
        Instant epochInMillisInstant = Instant.ofEpochMilli(startpointTimestamp.getTimestampOffset());
        EventPosition eventPosition = EventPosition.fromEnqueuedTime(epochInMillisInstant);
        String consumerGroup = eventHubConfig.getStreamConsumerGroup(systemStreamPartition.getSystem(), streamName);

        // 2. Create a partition receiver with event position defined by the timestamp.
        partitionReceiver = eventHubClient.createReceiverSync(consumerGroup, partitionId, eventPosition);

        // 3. Read a single message from the partition receiver.
        Iterable<EventData> eventHubMessagesIterator = partitionReceiver.receiveSync(1);
        ArrayList<EventData> eventHubMessageList = Lists.newArrayList(eventHubMessagesIterator);

        // 4. Validate that a single message was fetched from the broker.
        Preconditions.checkState(eventHubMessageList.size() == 1, "Failed to read messages from EventHub system.");

        // 5. Return the offset present in the metadata of the first message.
        return eventHubMessageList.get(0).getSystemProperties().getOffset();
      } catch (EventHubException e) {
        LOG.error(String.format("Exception occurred when fetching offset for timestamp: %d from the stream: %s", startpointTimestamp.getTimestampOffset(), streamName), e);
        throw new SamzaException(e);
      } finally {
        if (partitionReceiver != null) {
          try {
            partitionReceiver.closeSync();
          } catch (EventHubException e) {
            LOG.error(String.format("Exception occurred when closing partition-receiver of the stream: %s", streamName), e);
          }
        }
      }
    }

    @Override
    public String visit(SystemStreamPartition systemStreamPartition, StartpointOldest startpointOldest) {
      return EventHubSystemConsumer.START_OF_STREAM;
    }

    @Override
    public String visit(SystemStreamPartition systemStreamPartition, StartpointUpcoming startpointUpcoming) {
      return EventHubSystemConsumer.END_OF_STREAM;
    }
  }
}
