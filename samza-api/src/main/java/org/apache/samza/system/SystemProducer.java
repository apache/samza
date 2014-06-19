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

package org.apache.samza.system;

/**
 * SystemProducers are how Samza writes messages from {@link org.apache.samza.task.StreamTask}s to outside systems,
 * such as messaging systems like Kafka, or file systems.  Implementations are responsible for accepting messages
 * and writing them to their backing systems.
 */
public interface SystemProducer {

  /**
   * Start the SystemProducer. After this method finishes it should be ready to accept messages received from the send method.
   */
  void start();

  /**
   * Stop the SystemProducer. After this method finished, the system should have completed all necessary work, sent
   * any remaining messages and will not receive any new calls to the send method.
   */
  void stop();

  /**
   * Registers this producer to send messages from a specified Samza source, such as a StreamTask.
   * @param source String representing the source of the message.
   */
  void register(String source);

  /**
   * Sends a specified message envelope from a specified Samza source.
   * @param source String representing the source of the message.
   * @param envelope Aggregate object representing the serialized message to send from the source.
   */
  void send(String source, OutgoingMessageEnvelope envelope);

  /**
   * If the SystemProducer buffers messages before sending them to its underlying system, it should flush those
   * messages and leave no messages remaining to be sent.
   *
   * @param source String representing the source of the message.
   */
  void flush(String source);
}
