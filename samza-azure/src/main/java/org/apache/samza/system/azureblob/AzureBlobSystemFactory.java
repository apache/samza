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

package org.apache.samza.system.azureblob;

import org.apache.samza.system.azureblob.producer.AzureBlobSystemProducer;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Do not use this SystemProducer for Coordinator stream store/producer and KafkaCheckpointManager
 * as their usage of SystemProducer is a bit inconsistent with this implementation and they also couple
 * a SystemProducer with a SystemConsumer which is out of scope for this Factory.
 * {@inheritDoc}
 */
public class AzureBlobSystemFactory implements SystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobSystemFactory.class.getName());

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    throw new UnsupportedOperationException("SystemConsumer not supported for AzureBlob!");
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    AzureBlobConfig azureBlobConfig = new AzureBlobConfig(config);
    return new AzureBlobSystemProducer(systemName, azureBlobConfig, registry);
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new AzureBlobSystemAdmin();
  }
}
