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

import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;

import java.util.Set;
import org.apache.samza.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class which handles the common functionality for coordinator stream consumer and producer
 */
public class CoordinatorStreamManager {
  private Logger log = LoggerFactory.getLogger(CoordinatorStreamManager.class);
  private final String source;
  private CoordinatorStreamSystemProducer coordinatorStreamProducer;
  private CoordinatorStreamSystemConsumer coordinatorStreamConsumer;

  /**
   * Creates a new {@link CoordinatorStreamManager} with a given coordinator stream producer, consumer and with a given source.
   *
   * @param coordinatorStreamProducer The {@link CoordinatorStreamSystemProducer} which should be used with the {@link CoordinatorStreamManager}
   * @param coordinatorStreamConsumer The {@link CoordinatorStreamSystemConsumer} which should be used with the {@link CoordinatorStreamManager}
   * @param source The source of the coordinator stream.
   */
  public CoordinatorStreamManager(CoordinatorStreamSystemProducer coordinatorStreamProducer, CoordinatorStreamSystemConsumer coordinatorStreamConsumer, String source) {
    this.coordinatorStreamProducer = coordinatorStreamProducer;
    this.coordinatorStreamConsumer = coordinatorStreamConsumer;
    this.source = source;
  }

  /**
   * Creates a new {@link CoordinatorStreamManager} and instantiates the underlying coordinator stream producer and consumer.
   * @param coordinatorSystemConfig Configuration used to instantiate the coordinator stream producer and consumer.
   * @param metricsRegistry Metrics registry
   * @param source The source of the coordinator stream.
   */
  public CoordinatorStreamManager(Config coordinatorSystemConfig, MetricsRegistry metricsRegistry, String source) {
    coordinatorStreamConsumer = new CoordinatorStreamSystemConsumer(coordinatorSystemConfig, metricsRegistry);
    coordinatorStreamProducer = new CoordinatorStreamSystemProducer(coordinatorSystemConfig, metricsRegistry);
    this.source = source;
  }

  /**
   * Fully starts, registers and bootstraps the underlying coordinator stream producer and consumer.
   */
  public void registerStartBootstrapAll() {
    registerCoordinatorStreamConsumer();
    startCoordinatorStreamConsumer();
    bootstrapCoordinatorStreamConsumer();
    registerCoordinatorStreamProducer(source);
    startCoordinatorStreamProducer();
  }

  /**
   * Stops the underlying coordinator stream producer and consumer.
   */
  public void stop() {
    stopCoordinatorStreamConsumer();
    stopCoordinatorStreamProducer();
  }

  /**
   * Start the coordinator stream consumer.
   */
  public void startCoordinatorStreamConsumer() {
    if (coordinatorStreamConsumer != null && !coordinatorStreamConsumer.isStarted()) {
      log.debug("Starting coordinator system stream consumer.");
      coordinatorStreamConsumer.start();
    }
  }

  /**
   * Start the coordinator stream producer.
   */
  public void startCoordinatorStreamProducer() {
    if (coordinatorStreamProducer != null && !coordinatorStreamProducer.isStarted()) {
      log.debug("Starting coordinator system stream producer.");
      coordinatorStreamProducer.start();
    }
  }

  /**
   * Sends a {@link CoordinatorStreamMessage} using the underlying system producer.
   * @param message message which should be sent to producer
   */
  public void send(CoordinatorStreamMessage message) {
    if (coordinatorStreamProducer == null) {
      throw new UnsupportedOperationException(String.format("CoordinatorStreamProducer is not initialized in the CoordinatorStreamManager. "));
    }
    coordinatorStreamProducer.send(message);
  }

  /**
   * Bootstrap the coordinator stream consumer.
   */
  public void bootstrapCoordinatorStreamConsumer() {
    if (coordinatorStreamConsumer != null && !coordinatorStreamConsumer.isBootstrapped()) {
      log.debug("Bootstrapping coordinator system stream consumer.");
      coordinatorStreamConsumer.bootstrap();
    }
  }

  /**
   * Returns a set of messages from the bootstrapped stream for a given source.
   * @param source the source of the given messages
   * @return a set of {@link CoordinatorStreamMessage} if messages exists for the given source, else an empty set
   */
  public Set<CoordinatorStreamMessage> getBootstrappedStream(String source) {
    if (coordinatorStreamConsumer == null) {
      throw new UnsupportedOperationException(String.format("CoordinatorStreamConsumer is not initialized in the CoordinatorStreamManager. "
          + "input source: %s", source));
    }
    return coordinatorStreamConsumer.getBootstrappedStream(source);
  }

  /**
   * Register the coordinator stream consumer.
   */
  public void registerCoordinatorStreamConsumer() {
    if (coordinatorStreamConsumer != null) {
      log.info("Registering coordinator system stream consumer from {}.", source);
      coordinatorStreamConsumer.register();
    }
  }

  /**
   * Registers the coordinator stream producer for a given source.
   * @param source the source to register
   */
  public void registerCoordinatorStreamProducer(String source) {
    if (coordinatorStreamProducer != null) {
      log.info("Registering coordinator system stream producer from {}.", source);
      coordinatorStreamProducer.register(source);
    }
  }

  /**
   * Returns the config for the coordinator stream consumer.
   * @return Config of the coordinator stream consumer.
   */
  public Config getCoordinatorStreamConsumerConfig() {
    if (coordinatorStreamConsumer == null) {
      throw new UnsupportedOperationException(String.format("CoordinatorStreamConsumer is not initialized in the CoordinatorStreamManager. "
          + "input source: %s", source));
    }
    return coordinatorStreamConsumer.getConfig();
  }

  /**
   * Stop only the consumer.
   */
  public void stopCoordinatorStreamConsumer() {
    if (coordinatorStreamConsumer != null && coordinatorStreamConsumer.isStarted()) {
      coordinatorStreamConsumer.stop();
    }
  }

  /**
   * Stop only the producer.
   */
  public void stopCoordinatorStreamProducer() {
    if (coordinatorStreamProducer != null && coordinatorStreamProducer.isStarted()) {
      coordinatorStreamProducer.stop();
    }
  }

  /**
   * Source name of the coordinator stream.
   *
   * @return Source name.
   */
  public String getSource() {
    return source;
  }
}
