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

import java.util.Optional;
import org.apache.samza.Partition;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class InMemorySystemProducer implements SystemProducer {
  private static final Logger LOG = LoggerFactory.getLogger(InMemorySystemProducer.class);
  private final InMemoryManager memoryManager;
  private final String systemName;
  private final InMemorySystemConfig inMemorySystemConfig;
  private final MetricsRegistry metricsRegistry;

  public InMemorySystemProducer(String systemName, InMemorySystemConfig config, MetricsRegistry metricsRegistry, InMemoryManager manager) {
    this.systemName = systemName;
    this.metricsRegistry = metricsRegistry;
    this.inMemorySystemConfig = config;
    this.memoryManager = manager;
  }

  /**
   * Start the SystemProducer. After this method finishes it should be ready to accept messages received from the send method.
   */
  @Override
  public void start() {

  }

  /**
   * Stop the SystemProducer. After this method finished, the system should have completed all necessary work, sent
   * any remaining messages and will not receive any new calls to the send method.
   */
  @Override
  public void stop() {

  }

  /**
   * Registers this producer to send messages from a specified Samza source, such as a StreamTask.

   * @param source String representing the source of the message.
   */
  @Override
  public void register(String source) {

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

    // use the hashcode from partition key in the outgoing message envelope or default to message hashcode
    int hashCode = Optional.ofNullable(envelope.getPartitionKey())
        .map(Object::hashCode)
        .orElse(message.hashCode());
    int partition = hashCode % memoryManager.getPartitionCountForSystemStream(envelope.getSystemStream());

    SystemStreamPartition ssp = new SystemStreamPartition(envelope.getSystemStream(), new Partition(partition));
    memoryManager.put(ssp, key, message);
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
}
