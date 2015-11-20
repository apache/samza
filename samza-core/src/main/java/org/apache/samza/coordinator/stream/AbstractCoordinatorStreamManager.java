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

package org.apache.samza.coordinator.stream;

import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;

import java.util.Set;

/**
 * Abstract class which handles the common functionality for coordinator stream consumer and producer
 */
public abstract class AbstractCoordinatorStreamManager {
  private final CoordinatorStreamSystemProducer coordinatorStreamProducer;
  private final CoordinatorStreamSystemConsumer coordinatorStreamConsumer;
  private final String source;

  /**
   * Creates a new {@link AbstractCoordinatorStreamManager} with a given coordinator stream producer, consumer and with a given source.
   * @param coordinatorStreamProducer the {@link CoordinatorStreamSystemProducer} which should be used with the {@link AbstractCoordinatorStreamManager}
   * @param coordinatorStreamConsumer the {@link CoordinatorStreamSystemConsumer} which should be used with the {@link AbstractCoordinatorStreamManager}
   * @param source ths source for the coordinator stream producer
   */
  protected AbstractCoordinatorStreamManager(CoordinatorStreamSystemProducer coordinatorStreamProducer, CoordinatorStreamSystemConsumer coordinatorStreamConsumer, String source) {
    this.coordinatorStreamProducer = coordinatorStreamProducer;
    this.coordinatorStreamConsumer = coordinatorStreamConsumer;
    this.source = source;
  }

  /**
   * Starts the underlying coordinator stream producer and consumer.
   */
  public void start() {
    coordinatorStreamProducer.start();
    if (coordinatorStreamConsumer != null) {
      coordinatorStreamConsumer.start();
    }
  }

  /**
   * Stops the underlying coordinator stream producer and consumer.
   */
  public void stop() {
    if (coordinatorStreamConsumer != null) {
      coordinatorStreamConsumer.stop();
    }
    coordinatorStreamProducer.stop();
  }

  /**
   * Sends a {@link CoordinatorStreamMessage} using the underlying system producer.
   * @param message message which should be sent to producer
   */
  public void send(CoordinatorStreamMessage message) {
    coordinatorStreamProducer.send(message);
  }

  /**
   * Returns a set of messages from the bootstrapped stream for a given source.
   * @param source the source of the given messages
   * @return a set of {@link CoordinatorStreamMessage} if messages exists for the given source, else an empty set
   */
  public Set<CoordinatorStreamMessage> getBootstrappedStream(String source) {
    if (coordinatorStreamConsumer == null) {
      throw new UnsupportedOperationException(String.format("CoordinatorStreamConsumer is not initialized in the AbstractCoordinatorStreamManager. "
          + "manager registered source: %s, input source: %s", this.source, source));
    }
    return coordinatorStreamConsumer.getBootstrappedStream(source);
  }

  /**
   * Register the coordinator stream consumer.
   */
  protected void registerCoordinatorStreamConsumer() {
    coordinatorStreamConsumer.register();
  }

  /**
   * Registers the coordinator stream producer for a given source.
   * @param source the source to register
   */
  protected void registerCoordinatorStreamProducer(String source) {
    coordinatorStreamProducer.register(source);
  }

  /**
   * Returns the source name which is managed by {@link AbstractCoordinatorStreamManager}.
   * @return the source name
   */
  protected String getSource() {
    return source;
  }

  /**
   * Registers a consumer and a produces. Every subclass should implement it's logic for registration.<br><br>
   * Registering a single consumer and a single producer can be done with {@link AbstractCoordinatorStreamManager#registerCoordinatorStreamConsumer()}
   * and {@link AbstractCoordinatorStreamManager#registerCoordinatorStreamProducer(String)} methods respectively.<br>
   * These methods can be used in the concrete implementation of this register method.
   *
   * @param taskName name which should be used with the producer
   */
  public abstract void register(TaskName taskName);
}
