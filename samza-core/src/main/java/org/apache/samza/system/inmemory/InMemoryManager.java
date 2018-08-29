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

package org.apache.samza.system.inmemory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Initial draft of in-memory manager. It is test only and not meant for production use right now.
 */
class InMemoryManager {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryManager.class);
  private static final int DEFAULT_PARTITION_COUNT = 1;

  private final ConcurrentHashMap<SystemStreamPartition, List<IncomingMessageEnvelope>> bufferedMessages;
  private final Map<SystemStream, Integer> systemStreamToPartitions;

  public InMemoryManager() {
    bufferedMessages = new ConcurrentHashMap<>();
    systemStreamToPartitions = new ConcurrentHashMap<>();
  }

  private List<IncomingMessageEnvelope> newSynchronizedLinkedList() {
    return  Collections.synchronizedList(new LinkedList<IncomingMessageEnvelope>());
  }

  /**
   * Handles the produce request from {@link InMemorySystemProducer} and populates the underlying message queue.
   *
   * @param ssp system stream partition
   * @param key key for message produced
   * @param message actual payload
   */
  void put(SystemStreamPartition ssp, Object key, Object message) {
    List<IncomingMessageEnvelope> messages = bufferedMessages.get(ssp);
    String offset = String.valueOf(messages.size());

    if (message instanceof EndOfStreamMessage) {
      offset = IncomingMessageEnvelope.END_OF_STREAM_OFFSET;
    }

    IncomingMessageEnvelope messageEnvelope = new IncomingMessageEnvelope(ssp, offset, key, message);
    bufferedMessages.get(ssp)
        .add(messageEnvelope);
  }

  /**
   * Handles the poll request from {@link InMemorySystemConsumer}. It uses the input offset as the starting offset for
   * each {@link SystemStreamPartition}.
   *
   * @param sspsToOffsets ssps to offset mapping
   *
   * @return a {@link Map} of {@link SystemStreamPartition} to {@link List} of {@link IncomingMessageEnvelope}
   */
  Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Map<SystemStreamPartition, String> sspsToOffsets) {
    return sspsToOffsets.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> poll(entry.getKey(), entry.getValue())));
  }

  /**
   * Populate the metadata for the {@link SystemStream} and initialize the buffer for {@link SystemStreamPartition}.
   *
   * @param streamSpec stream spec for the stream to be initialized
   *
   * @return true if successful, false otherwise
   */
  boolean initializeStream(StreamSpec streamSpec) {
    LOG.info("Initializing the stream for {}", streamSpec.getId());
    systemStreamToPartitions.put(streamSpec.toSystemStream(), streamSpec.getPartitionCount());

    for (int partition = 0; partition < streamSpec.getPartitionCount(); partition++) {
      bufferedMessages.put(
          new SystemStreamPartition(streamSpec.toSystemStream(), new Partition(partition)),
          newSynchronizedLinkedList());
    }

    return true;
  }

  /**
   * Fetch system stream metadata for the given streams.
   *
   * @param systemName system name
   * @param streamNames set of input streams
   *
   * @return a {@link Map} of stream to {@link SystemStreamMetadata}
   */
  Map<String, SystemStreamMetadata> getSystemStreamMetadata(String systemName, Set<String> streamNames) {
    Map<String, Map<SystemStreamPartition, List<IncomingMessageEnvelope>>> result =
        bufferedMessages.entrySet()
            .stream()
            .filter(entry -> systemName.equals(entry.getKey().getSystem()) 
                && streamNames.contains(entry.getKey().getStream()))
            .collect(Collectors.groupingBy(entry -> entry.getKey().getStream(),
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    return result.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> constructSystemStreamMetadata(entry.getKey(), entry.getValue())));
  }

  /**
   * Fetch partition count for the given input stream stream.
   *
   * @param systemStream input system stream
   *
   * @return the partition count if available or {@link InMemoryManager#DEFAULT_PARTITION_COUNT}
   */
  int getPartitionCountForSystemStream(SystemStream systemStream) {
    return systemStreamToPartitions.getOrDefault(systemStream, DEFAULT_PARTITION_COUNT);
  }

  private SystemStreamMetadata constructSystemStreamMetadata(
      String streamName,
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> sspToMessagesForSystem) {

    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata =
        sspToMessagesForSystem
            .entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getPartition(), entry -> {
                String oldestOffset = "0";
                String newestOffset = String.valueOf(entry.getValue().size());
                String upcomingOffset = String.valueOf(entry.getValue().size() + 1);

                return new SystemStreamMetadata.SystemStreamPartitionMetadata(oldestOffset, newestOffset, upcomingOffset);

              }));

    return new SystemStreamMetadata(streamName, partitionMetadata);
  }

  private List<IncomingMessageEnvelope> poll(SystemStreamPartition ssp, String offset) {
    int startingOffset = Integer.parseInt(offset);
    List<IncomingMessageEnvelope> messageEnvelopesForSSP = bufferedMessages.getOrDefault(ssp, new LinkedList<>());

    if (startingOffset >= messageEnvelopesForSSP.size()) {
      return new ArrayList<>();
    }

    return messageEnvelopesForSSP.subList(startingOffset, messageEnvelopesForSSP.size());
  }
}
