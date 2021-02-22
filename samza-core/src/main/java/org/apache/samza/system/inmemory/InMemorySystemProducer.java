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

import com.google.common.base.Preconditions;
import java.util.Arrays;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemorySystemProducer implements SystemProducer {
  private static final Logger LOG = LoggerFactory.getLogger(InMemorySystemProducer.class);
  private final InMemoryManager memoryManager;
  private final String systemName;

  public InMemorySystemProducer(String systemName, InMemoryManager manager) {
    this.systemName = systemName;
    this.memoryManager = manager;
  }

  /**
   * Start the SystemProducer. After this method finishes it should be ready to accept messages received from the send method.
   */
  @Override
  public void start() {
    LOG.info("Starting in memory system producer for {}", systemName);
  }

  /**
   * Stop the SystemProducer. After this method finished, the system should have completed all necessary work, sent
   * any remaining messages and will not receive any new calls to the send method.
   */
  @Override
  public void stop() {
    LOG.info("Stopping in memory system producer for {}", systemName);
  }

  /**
   * Registers this producer to send messages from a specified Samza source, such as a StreamTask.

   * @param source String representing the source of the message.
   */
  @Override
  public void register(String source) {
    LOG.info("Registering source {} with in memory producer", source);
  }

  /**
   * Sends a specified message envelope from a specified Samza source.

   * @param source String representing the source of the message.
   * @param envelope Aggregate object representing the serialized message to send from the source.
   */
  @Override
  public void send(String source, OutgoingMessageEnvelope envelope) {
    Object key = envelope.getKey();
    Object message = envelope.getMessage();

    Object partitionKey;
    // We use the partition key from message if available, if not fallback to message key or use message as partition
    // key as the final resort.
    if (envelope.getPartitionKey() != null) {
      partitionKey = envelope.getPartitionKey();
    } else if (key != null) {
      partitionKey = key;
    } else {
      partitionKey = message;
    }

    Preconditions.checkNotNull(partitionKey, "Failed to compute partition key for the message: " + envelope);

    int partition =
        Math.abs(hashCode(partitionKey)) % memoryManager.getPartitionCountForSystemStream(envelope.getSystemStream());

    SystemStreamPartition ssp = new SystemStreamPartition(envelope.getSystemStream(), new Partition(partition));
    memoryManager.put(ssp, key, message);
  }

  /**
   * Populates the IME to the ssp configured, this gives user more control to set up Test environment partition.
   * The offset in the envelope needs to adhere to a rule that for messages in the same system stream partition the
   * offset needs to start at 0 for the first and be monotonically increasing for the following messages.
   * If not the {@link InMemoryManager#put(SystemStreamPartition, IncomingMessageEnvelope)} will fail.
   *
   * Note: Please DO NOT use this in production use cases, this is only meant to set-up more flexible tests.
   * This function is not thread safe.
   * @param envelope incoming message envelope
   */
  public void send(IncomingMessageEnvelope envelope) {
    memoryManager.put(envelope.getSystemStreamPartition(), envelope);
  }

  /**
   * If the SystemProducer buffers messages before sending them to its underlying system, it should flush those
   * messages and leave no messages remaining to be sent.
   *

   * @param source String representing the source of the message.
   */
  @Override
  public void flush(String source) {
    // nothing to do
  }

  /**
   * Return the hash code of the partitionKey. When partitionKey is a byte array, it returns a hash code based on
   * the contents of the byte array. This guarantees that byte arrays with same contents get the same hash code.
   */
  private int hashCode(Object partitionKey) {
    return (partitionKey instanceof byte[]) ? Arrays.hashCode((byte[]) partitionKey) : partitionKey.hashCode();
  }
}
