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

package org.apache.samza.system.eventhub.consumer;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import org.apache.samza.system.eventhub.EventHubConfig;

import java.util.List;
import java.util.Map;

class MockEventHubEntityConnectionFactory extends EventHubEntityConnectionFactory {

  private final Map<String, Map<Integer, List<EventData>>> eventData;

  MockEventHubEntityConnectionFactory(Map<String, Map<Integer, List<EventData>>> eventData) {
    this.eventData = eventData;
  }

  @Override
  EventHubEntityConnection createConnection(String namespace, String entityPath, String sasKeyName, String sasKey,
                                            String consumerName, EventHubConfig eventHubConfig) {
    return new MockEventHubEntityConnection(entityPath, eventData.get(entityPath));
  }

  private class MockEventHubEntityConnection extends EventHubEntityConnection {
    private final Map<Integer, List<EventData>> eventData;

    MockEventHubEntityConnection(String entity, Map<Integer, List<EventData>> eventData) {
      super(null, entity, null, null, null, null);
      assert eventData != null : "No event data found for entity:" + entity;
      this.eventData = eventData;
    }

    @Override
    void connectAndStart() {
      offsets.keySet().forEach(partitionId -> {
          List<EventData> events = eventData.get(partitionId);
          PartitionReceiveHandler partitionHandler = handlers.get(partitionId);
          assert events != null : String.format("partition %d not found", partitionId);
          assert partitionHandler != null : String.format("handler %d not registered", partitionId);
          partitionHandler.onReceive(events);
        });
    }

    @Override
    void stop() {
      // do nothing
    }
  }
}
