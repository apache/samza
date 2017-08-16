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
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.task.MessageCollector;


/**
 * This is a helper class to broadcast control messages to each partition of an intermediate stream
 */
class ControlMessageSender {
  private final StreamMetadataCache metadataCache;

  ControlMessageSender(StreamMetadataCache metadataCache) {
    this.metadataCache = metadataCache;
  }

  void send(ControlMessage message, SystemStream systemStream, MessageCollector collector) {
    SystemStreamMetadata metadata = metadataCache.getSystemStreamMetadata(systemStream, true);
    int partitionCount = metadata.getSystemStreamPartitionMetadata().size();
    for (int i = 0; i < partitionCount; i++) {
      OutgoingMessageEnvelope envelopeOut = new OutgoingMessageEnvelope(systemStream, i, "", message);
      collector.send(envelopeOut);
    }
  }
}
