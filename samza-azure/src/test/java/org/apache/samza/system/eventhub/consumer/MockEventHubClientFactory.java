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
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.EventHubClientWrapper;
import org.apache.samza.system.eventhub.EventHubClientWrapperFactory;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.junit.Assert;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;

public class MockEventHubClientFactory extends EventHubClientWrapperFactory {
  private Map<SystemStreamPartition, List<EventData>> eventData;

  MockEventHubClientFactory(Map<SystemStreamPartition, List<EventData>> eventData) {
    this.eventData = eventData;
  }

  @Override
  public EventHubClientWrapper getEventHubClientWrapper(String eventHubNamespace, String entityPath, String sasKeyName,
                                                        String sasToken, EventHubConfig config) {
    return new MockEventHubClientWrapper();
  }

  // Emulate EventHub sending data
  void sendToHandlers(Map<SystemStreamPartition, PartitionReceiveHandler> handlers) {
    handlers.forEach((ssp, value) -> value.onReceive(eventData.get(ssp)));
  }

  private class MockEventHubClientWrapper implements EventHubClientWrapper {
    Boolean initiated = false;
    EventHubClient mockEventHubClient = PowerMockito.mock(EventHubClient.class);

    MockEventHubClientWrapper() {
      PartitionReceiver mockPartitionReceiver = PowerMockito.mock(PartitionReceiver.class);

      // Set mocks
      PowerMockito.when(mockPartitionReceiver.setReceiveHandler(any())).then((Answer<Void>) invocationOnMock -> {
          PartitionReceiveHandler handler = invocationOnMock.getArgumentAt(0, PartitionReceiveHandler.class);
          if (handler == null) {
            Assert.fail("Handler for setReceiverHandler was null");
          }
          return null;
        });

      try {
        PowerMockito.when(mockEventHubClient.createReceiverSync(anyString(), anyString(), any(Instant.class)))
                .thenReturn(mockPartitionReceiver);
        PowerMockito.when(mockEventHubClient.createReceiverSync(anyString(), anyString(), anyString(), anyBoolean()))
                .thenReturn(mockPartitionReceiver);
      } catch (Exception e) {
        Assert.fail("Cannot create mockReceiverSync");
      }
    }

    @Override
    public void init() {
      initiated = true;
    }

    @Override
    public EventHubClient getEventHubClient() {
      if (!initiated) {
        Assert.fail("Should have called init() on EventHubClient before getEventHubClientWrapper()");
      }
      return mockEventHubClient;
    }

    @Override
    public void close(long timeoutMS) {
      if (!initiated) {
        Assert.fail("Should have called init() on EventHubClient before close()");
      }
      initiated = false;
    }

  }

}
