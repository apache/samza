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
import org.apache.samza.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Encapsulates the creation and management of the coordinator stream producer and consumer.
 */
public class CoordinatorStream {
  private Logger log = LoggerFactory.getLogger(CoordinatorStream.class);
  private final Config config;
  private final MetricsRegistry metricsRegistry;
  private final String source;
  private CoordinatorStreamSystemProducer producer;
  private CoordinatorStreamSystemConsumer consumer;

  /**
   * Construct the CoordinatorStream
   *
   * @param coordinatorSystemConfig Config with coordinator properties
   * @param metricsRegistry Metrics for the consumer and producer to use.
   * @param source Describes the caller that creates the coordinator stream.
   */
  public CoordinatorStream(Config coordinatorSystemConfig, MetricsRegistry metricsRegistry, String source) {
    this.config = coordinatorSystemConfig;
    this.metricsRegistry = metricsRegistry;
    this.source = source;
  }

  /**
   * Construct and bootstrap the coordinator stream consumer.
   *
   * @return True if consumer is constructed and bootstrap. False if this was already done.
   */
  public boolean startConsumer() {
    if (this.consumer == null) {
      consumer = new CoordinatorStreamSystemConsumer(config, metricsRegistry);
      log.info("Registering coordinator system stream consumer from {}.", source);
      consumer.register();
      log.debug("Starting coordinator system stream consumer.");
      consumer.start();
      log.debug("Bootstrapping coordinator system stream consumer.");
      consumer.bootstrap();

      return true;
    }
    return false;
  }

  /**
   * Construct and bootstrap the coordinator stream producer.
   *
   * @return True if producer is constructed and bootstrap. False if this was already done.
   */
  public boolean startProducer() {
    if (producer == null) {
      producer = new CoordinatorStreamSystemProducer(config, metricsRegistry);
      log.info("Registering coordinator system stream consumer from {}.", source);
      producer.register(this.source);
      log.debug("Starting coordinator system stream consumer.");
      producer.start();
      log.debug("Bootstrapping coordinator system stream consumer.");

      return true;
    }
    return false;
  }

  /**
   * Get coordinator stream producer.
   *
   * @return CoordinatorStreamSystemProducer
   */
  public CoordinatorStreamSystemProducer getProducer() {
    return producer;
  }

  /**
   * Get coordinator stream consumer.
   *
   * @return CoordinatorStreamSystemConsumer
   */
  public CoordinatorStreamSystemConsumer getConsumer() {
    return consumer;
  }

  /**
   * Name of the caller that creates the coordinator stream.
   *
   * @return Source name of caller.
   */
  public String getSource() {
    return source;
  }
}
