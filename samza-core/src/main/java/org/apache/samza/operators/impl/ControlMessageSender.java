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

package org.apache.samza.operators.impl;

import org.apache.samza.SamzaException;
import org.apache.samza.system.ControlMessage;
import org.apache.samza.system.MessageType;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This is a helper class to send control messages to an intermediate stream
 */
class ControlMessageSender {
  private static final Logger LOG = LoggerFactory.getLogger(ControlMessageSender.class);
  private static final Map<SystemStream, Integer> PARTITION_COUNT_CACHE = new ConcurrentHashMap<>();

  private final StreamMetadataCache metadataCache;

  ControlMessageSender(StreamMetadataCache metadataCache) {
    this.metadataCache = metadataCache;
  }

  void send(ControlMessage message, SystemStream systemStream, MessageCollector collector) {
    int partitionCount = getPartitionCount(systemStream);
    // We pick a partition based on topic hashcode to aggregate the control messages from upstream tasks
    // After aggregation the task will broadcast the results to other partitions
    int aggregatePartition = systemStream.getStream().hashCode() % partitionCount;

    LOG.debug(String.format("Send %s message from task %s to %s partition %s for aggregation",
        MessageType.of(message).name(), message.getTaskName(), systemStream, aggregatePartition));

    OutgoingMessageEnvelope envelopeOut = new OutgoingMessageEnvelope(systemStream, aggregatePartition, null, message);
    collector.send(envelopeOut);
  }

  void broadcastToOtherPartitions(ControlMessage message, SystemStreamPartition ssp, MessageCollector collector) {
    SystemStream systemStream = ssp.getSystemStream();
    int partitionCount = getPartitionCount(systemStream);
    int currentPartition = ssp.getPartition().getPartitionId();
    for (int i = 0; i < partitionCount; i++) {
      if (i != currentPartition) {
        OutgoingMessageEnvelope envelopeOut = new OutgoingMessageEnvelope(systemStream, i, null, message);
        collector.send(envelopeOut);
      }
    }
  }

  private int getPartitionCount(SystemStream systemStream) {
    return PARTITION_COUNT_CACHE.computeIfAbsent(systemStream, ss -> {
        SystemStreamMetadata metadata = metadataCache.getSystemStreamMetadata(ss, true);
        if (metadata == null) {
          throw new SamzaException("Unable to find metadata for stream " + systemStream);
        }
        return metadata.getSystemStreamPartitionMetadata().size();
      });
  }
}
