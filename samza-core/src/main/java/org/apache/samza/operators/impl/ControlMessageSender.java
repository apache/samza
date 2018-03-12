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

import org.apache.samza.system.ControlMessage;
import org.apache.samza.system.MessageType;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.task.MessageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a helper class to broadcast control messages to each partition of an intermediate stream
 */
class ControlMessageSender {
  private static final Logger LOG = LoggerFactory.getLogger(ControlMessageSender.class);

  private final StreamMetadataCache metadataCache;

  ControlMessageSender(StreamMetadataCache metadataCache) {
    this.metadataCache = metadataCache;
  }

  void send(ControlMessage message, SystemStream systemStream, MessageCollector collector) {
    SystemStreamMetadata metadata = metadataCache.getSystemStreamMetadata(systemStream, false);
    int partitionCount = metadata.getSystemStreamPartitionMetadata().size();
    LOG.debug(String.format("Broadcast %s message from task %s to %s with %s partition",
        MessageType.of(message).name(), message.getTaskName(), systemStream, partitionCount));

    for (int i = 0; i < partitionCount; i++) {
      OutgoingMessageEnvelope envelopeOut = new OutgoingMessageEnvelope(systemStream, i, null, message);
      collector.send(envelopeOut);
    }
  }
}
