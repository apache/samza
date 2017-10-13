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

package org.apache.samza.system.eventhub;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.ByteSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.eventhub.admin.EventHubSystemAdmin;
import org.apache.samza.system.eventhub.consumer.EventHubSystemConsumer;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventHubSystemFactory implements SystemFactory {

  private Map<String, Serde<byte[]>> getSerdesMap(EventHubConfig config, String systemName) {
    Map<String, Serde<byte[]>> serdes = new HashMap<>();
    List<String> streamList = config.getStreams(systemName);
    streamList.forEach((streamName) -> serdes.put(streamName, new ByteSerde()));
    return serdes;
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    EventHubConfig eventHubConfig = new EventHubConfig(config);
    return new EventHubSystemConsumer(eventHubConfig, systemName, new SamzaEventHubClientFactory(),
            getSerdesMap(eventHubConfig, systemName), registry);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    EventHubConfig eventHubConfig = new EventHubConfig(config);
    return new EventHubSystemProducer(eventHubConfig, systemName, new SamzaEventHubClientFactory(),
            getSerdesMap(eventHubConfig, systemName),
            registry);
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new EventHubSystemAdmin(systemName, new EventHubConfig(config), new SamzaEventHubClientFactory());
  }
}
