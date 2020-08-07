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

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;

/**
 * Build the {@link org.apache.samza.system.SystemConsumer} and {@link org.apache.samza.system.SystemProducer} for
 * a particular system, as well as the accompanying {@link org.apache.samza.system.SystemAdmin}.
 */
public interface SystemFactory {
  SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry);

  SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry);

  SystemAdmin getAdmin(String systemName, Config config);

  /**
   * This function provides an extra input parameter than {@link #getConsumer}, which can be used to provide extra
   * information e.g. ownership of client instance
   *
   * @param systemName The name of the system to create consumer for.
   * @param config The config to create consumer with.
   * @param registry MetricsRegistry to which to publish consumer specific metrics.
   * @param consumerLabel a string used to provide info the consumer instance.
   * @return A SystemConsumer
   */
  default SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry, String consumerLabel) {
    return getConsumer(systemName, config, registry);
  }

  /**
   * This function provides an extra input parameter than {@link #getProducer}, which can be used to provide extra
   * information e.g. ownership of client instance
   *
   * @param systemName The name of the system to create producer for.
   * @param config The config to create producer with.
   * @param registry MetricsRegistry to which to publish producer specific metrics.
   * @param producerLabel a string used to provide info the producer instance.
   * @return A SystemProducer
   */
  default SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry, String producerLabel) {
    return getProducer(systemName, config, registry);
  }

  /**
   *This function provides an extra input parameter than {@link #getAdmin}, which can be used to provide extra
   * information e.g. ownership of client instance
   *
   * @param systemName The name of the system to create admin for.
   * @param config The config to create admin with.
   * @param adminLabel a string used to provide info the admin instance.
   * @return A SystemAmind
   */
  default SystemAdmin getAdmin(String systemName, Config config, String adminLabel) {
    return getAdmin(systemName, config);
  }
}
