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
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;

import java.util.HashMap;

public class MockEventHubConfigFactory {

  public static final String SYSTEM_NAME = "eventhub-s1";
  public static final String STREAM_NAME1 = "test_stream1";
  public static final String STREAM_NAME2 = "test_stream2";

  // Add target Event Hub connection information for integration test here
  public static final String EVENTHUB_NAMESPACE = "";
  public static final String EVENTHUB_KEY_NAME = "";
  public static final String EVENTHUB_KEY = "";
  public static final String EVENTHUB_ENTITY1 = "";
  public static final String EVENTHUB_ENTITY2 = "";

  public static final int MIN_EVENTHUB_ENTITY_PARTITION = 2;
  public static final int MAX_EVENTHUB_ENTITY_PARTITION = 32;

  public static Config getEventHubConfig(EventHubSystemProducer.PartitioningMethod partitioningMethod) {
    HashMap<String, String> mapConfig = new HashMap<>();
    mapConfig.put(String.format(EventHubConfig.CONFIG_PRODUCER_PARTITION_METHOD, SYSTEM_NAME), partitioningMethod.toString());
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, SYSTEM_NAME), STREAM_NAME1 + "," + STREAM_NAME2);

    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, STREAM_NAME1), EVENTHUB_NAMESPACE);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, STREAM_NAME1), EVENTHUB_ENTITY1);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, STREAM_NAME1), EVENTHUB_KEY_NAME);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, STREAM_NAME1), EVENTHUB_KEY);

    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, STREAM_NAME2), EVENTHUB_NAMESPACE);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, STREAM_NAME2), EVENTHUB_ENTITY2);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, STREAM_NAME2), EVENTHUB_KEY_NAME);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, STREAM_NAME2), EVENTHUB_KEY);

    return new MapConfig(mapConfig);
  }
}
