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

import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which handles the common functionality for coordinator stream consumer and producer
 */
public class CoordinatorStreamManager {
  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorStreamManager.class);

  private final CoordinatorStreamSystemProducer coordinatorStreamProducer;
  private final CoordinatorStreamSystemConsumer coordinatorStreamConsumer;

  /**
   * Creates a new {@link CoordinatorStreamManager} with a given coordinator stream producer and consumer.
   *
   * @param coordinatorStreamProducer The {@link CoordinatorStreamSystemProducer} which should be used with the {@link CoordinatorStreamManager}
   * @param coordinatorStreamConsumer The {@link CoordinatorStreamSystemConsumer} which should be used with the {@link CoordinatorStreamManager}
   */
  public CoordinatorStreamManager(CoordinatorStreamSystemProducer coordinatorStreamProducer,
      CoordinatorStreamSystemConsumer coordinatorStreamConsumer) {
    this.coordinatorStreamProducer = coordinatorStreamProducer;
    this.coordinatorStreamConsumer = coordinatorStreamConsumer;
  }

  /**
   * Creates a new {@link CoordinatorStreamManager} and instantiates the underlying coordinator stream producer and consumer.
   *
   * @param coordinatorSystemConfig Configuration used to instantiate the coordinator stream producer and consumer.
   * @param metricsRegistry Metrics registry
   */
  public CoordinatorStreamManager(Config coordinatorSystemConfig, MetricsRegistry metricsRegistry) {
    coordinatorStreamConsumer = new CoordinatorStreamSystemConsumer(coordinatorSystemConfig, metricsRegistry);
    coordinatorStreamProducer = new CoordinatorStreamSystemProducer(coordinatorSystemConfig, metricsRegistry);
  }

  /**
   * Register source with the coordinator stream.
   *
   * @param source
   */
  public void register(String source) {
    if (coordinatorStreamConsumer != null) {
      LOG.info("Registering coordinator system stream consumer from {}.", source);
      coordinatorStreamConsumer.register();
    }
    if (coordinatorStreamProducer != null) {
      LOG.info("Registering coordinator system stream producer from {}.", source);
      coordinatorStreamProducer.register(source);
    }
  }

  /**
   * Starts the underlying coordinator stream producer and consumer.
   */
  public void start() {
    if (coordinatorStreamConsumer != null) {
      LOG.debug("Starting coordinator system stream consumer.");
      coordinatorStreamConsumer.start();
    }
    if (coordinatorStreamProducer != null) {
      LOG.debug("Starting coordinator system stream producer.");
      coordinatorStreamProducer.start();
    }
  }

  /**
   * Stops the underlying coordinator stream producer and consumer.
   */
  public void stop() {
    if (coordinatorStreamConsumer != null) {
      coordinatorStreamConsumer.stop();
    }
    if (coordinatorStreamProducer != null) {
      coordinatorStreamProducer.stop();
    }
  }

  /**
   * Sends a {@link CoordinatorStreamMessage} using the underlying system producer.
   *
   * @param message message which should be sent to producer
   */
  public void send(CoordinatorStreamMessage message) {
    if (coordinatorStreamProducer == null) {
      throw new UnsupportedOperationException(
          String.format("CoordinatorStreamProducer is not initialized in the CoordinatorStreamManager. "));
    }
    coordinatorStreamProducer.send(message);
  }

  /**
   * Bootstrap the coordinator stream consumer.
   */
  public void bootstrap() {
    if (coordinatorStreamConsumer != null) {
      LOG.debug("Bootstrapping coordinator system stream consumer.");
      coordinatorStreamConsumer.bootstrap();
    }
  }

  /**
   * Returns a set of messages from the bootstrapped stream for a given source.
   *
   * @param source the source of the given messages
   * @return a set of {@link CoordinatorStreamMessage} if messages exists for the given source, else an empty set
   */
  public Set<CoordinatorStreamMessage> getBootstrappedStream(String source) {
    if (coordinatorStreamConsumer == null) {
      throw new UnsupportedOperationException(
          String.format("CoordinatorStreamConsumer is not initialized in the CoordinatorStreamManager. "));
    }
    return coordinatorStreamConsumer.getBootstrappedStream(source);
  }

  /**
   * Returns the config from the coordinator stream consumer.
   *
   * @return Config of the coordinator stream consumer.
   */
  public Config getConfig() {
    if (coordinatorStreamConsumer == null) {
      throw new IllegalStateException(
          String.format("CoordinatorStreamConsumer is not initialized in the CoordinatorStreamManager. "));
    }
    return coordinatorStreamConsumer.getConfig();
  }
}
