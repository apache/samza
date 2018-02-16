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
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class InMemoryManager {
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

  public void put(SystemStreamPartition ssp, Object message) {
    put(ssp, null, message);
  }

  public void put(SystemStreamPartition ssp, Object key, Object message) {
    List<IncomingMessageEnvelope> messages = bufferedMessages.get(ssp);
    int offset = messages.size();
    IncomingMessageEnvelope messageEnvelope = new IncomingMessageEnvelope(ssp, String.valueOf(offset), key, message);
    bufferedMessages.get(ssp)
        .add(messageEnvelope);
  }

  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Map<SystemStreamPartition, String> sspsToOffsets) {
    return sspsToOffsets.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> poll(entry.getKey(), entry.getValue())));
  }

  private List<IncomingMessageEnvelope> poll(SystemStreamPartition ssp, String offset) {
    int startingOffset = Integer.parseInt(offset);
    List<IncomingMessageEnvelope> messageEnvelopesForSSP = bufferedMessages.getOrDefault(ssp, new LinkedList<>());

    if (startingOffset >= messageEnvelopesForSSP.size()) {
      return new ArrayList<>();
    }

    return messageEnvelopesForSSP.subList(startingOffset, messageEnvelopesForSSP.size());
  }

  /**
   * Populate the metadata for the {@link SystemStream} and initialize the buffer for {@link SystemStreamPartition}.
   *
   * @param streamSpec stream spec for the stream to be initialized
   *
   * @return true if successful, false otherwise
   */
  public boolean initializeStream(StreamSpec streamSpec) {
    systemStreamToPartitions.put(streamSpec.toSystemStream(), streamSpec.getPartitionCount());

    for (int partition = 0; partition < streamSpec.getPartitionCount(); partition++) {
      bufferedMessages.put(
          new SystemStreamPartition(streamSpec.toSystemStream(), new Partition(partition)),
          newSynchronizedLinkedList());
    }

    return true;
  }

  public boolean initializeStream(StreamSpec streamSpec, String serializedDataSet) {
    boolean populateStream = false;
    boolean initializeMetadata = initializeStream(streamSpec);
    try {
      int partition;
      Set<Object> dataSet = InMemorySystemUtils.deserialize(serializedDataSet);

      for (Object data : dataSet) {
        partition = data.hashCode() % streamSpec.getPartitionCount();
        put(new SystemStreamPartition(streamSpec.toSystemStream(), new Partition(partition)), data);
      }

      populateStream = true;
    } catch (Exception e) {
      LOG.error("Unable to initialize the stream due to deserialization error.", e);
    }

    return populateStream && initializeMetadata;
  }

  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    Map<String, Map<SystemStreamPartition, List<IncomingMessageEnvelope>>> result =
        bufferedMessages.entrySet()
            .stream()
            .filter(map -> streamNames.contains(map.getKey().getStream()))
            .collect(Collectors.groupingBy(entry -> entry.getKey().getStream(),
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    return result.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> constructSystemStreamMetadata(entry.getKey(), entry.getValue())));
  }

  public int getPartitionCountForSystemStream(SystemStream systemStream) {
    return systemStreamToPartitions.getOrDefault(systemStream, DEFAULT_PARTITION_COUNT);
  }

  private SystemStreamMetadata constructSystemStreamMetadata(
      String systemName,
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

    return new SystemStreamMetadata(systemName, partitionMetadata);
  }
}
