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

import com.microsoft.azure.eventhubs.*;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.*;

public class MockEventHubClientManagerFactory extends EventHubClientManagerFactory {
  private Map<SystemStreamPartition, List<EventData>> eventData;
  private Map<String, Map<String, Map<Integer, List<EventData>>>> receivedData;

  public MockEventHubClientManagerFactory() {
    this.receivedData = new HashMap<>();
  }

  public MockEventHubClientManagerFactory(Map<SystemStreamPartition, List<EventData>> eventData) {
    this.eventData = eventData;
  }

  @Override
  public EventHubClientManager getEventHubClientManager(String systemName, String streamName, EventHubConfig config) {
    if (receivedData != null) {
      if (!receivedData.containsKey(systemName)) {
        receivedData.put(systemName, new HashMap<>());
      }

      if (!receivedData.get(systemName).containsKey(streamName)) {
        receivedData.get(systemName).put(streamName, new HashMap<>());
        receivedData.get(systemName).get(streamName).put(0, new ArrayList<>());
        receivedData.get(systemName).get(streamName).put(1, new ArrayList<>());
      }
    }
    return new MockEventHubClientManager(systemName, streamName);
  }

  // Emulate EventHub sending data
  public void sendToHandlers(Map<SystemStreamPartition, PartitionReceiveHandler> handlers) {
    if (eventData == null) return;
    handlers.forEach((ssp, value) -> value.onReceive(eventData.get(ssp)));
  }

  public List<EventData> getSentData(String systemName, String streamName, Integer partitionId) {
    if (receivedData.containsKey(systemName) && receivedData.get(systemName).containsKey(streamName)) {
      return receivedData.get(systemName).get(streamName).get(partitionId);
    }
    return null;
  }

  private class MockEventHubClientManager implements EventHubClientManager {
    Boolean initiated = false;
    EventHubClient mockEventHubClient = PowerMockito.mock(EventHubClient.class);
    String systemName;
    String streamName;

    MockEventHubClientManager(String systemName, String streamName) {
      this.systemName = systemName;
      this.streamName = streamName;

      // Consumer mocks
      PartitionReceiver mockPartitionReceiver = PowerMockito.mock(PartitionReceiver.class);
      PowerMockito.when(mockPartitionReceiver.setReceiveHandler(any())).then((Answer<Void>) invocationOnMock -> {
          PartitionReceiveHandler handler = invocationOnMock.getArgumentAt(0, PartitionReceiveHandler.class);
          if (handler == null) {
            Assert.fail("Handler for setReceiverHandler was null");
          }
          return null;
        });

      // Producer mocks
      PartitionSender mockPartitionSender0 = PowerMockito.mock(PartitionSender.class);
      PartitionSender mockPartitionSender1 = PowerMockito.mock(PartitionSender.class);
      PowerMockito.when(mockPartitionSender0.send(any(EventData.class)))
              .then((Answer<CompletableFuture<Void>>) invocationOnMock -> {
                  EventData data = invocationOnMock.getArgumentAt(0, EventData.class);
                  receivedData.get(systemName).get(streamName).get(0).add(data);
                  return new CompletableFuture<>();
                });
      PowerMockito.when(mockPartitionSender1.send(any(EventData.class)))
              .then((Answer<CompletableFuture<Void>>) invocationOnMock -> {
                  EventData data = invocationOnMock.getArgumentAt(0, EventData.class);
                  receivedData.get(systemName).get(streamName).get(1).add(data);
                  return new CompletableFuture<>();
                });

      EventHubRuntimeInformation mockRuntimeInfo = PowerMockito.mock(EventHubRuntimeInformation.class);
      CompletableFuture<EventHubRuntimeInformation> future =  new MockFuture(mockRuntimeInfo);
      PowerMockito.when(mockRuntimeInfo.getPartitionCount()).thenReturn(2);

      try {
        // Consumer calls
        PowerMockito.when(mockEventHubClient.createReceiverSync(anyString(), anyString(), any(Instant.class)))
                .thenReturn(mockPartitionReceiver);
        PowerMockito.when(mockEventHubClient.createReceiverSync(anyString(), anyString(), anyString(), anyBoolean()))
                .thenReturn(mockPartitionReceiver);

        // Producer calls
        PowerMockito.when(mockEventHubClient.createPartitionSenderSync("0")).thenReturn(mockPartitionSender0);
        PowerMockito.when(mockEventHubClient.createPartitionSenderSync("1")).thenReturn(mockPartitionSender1);

        PowerMockito.when(mockEventHubClient.getRuntimeInformation()).thenReturn(future);

        PowerMockito.when(mockEventHubClient.send(any(EventData.class), anyString()))
                .then((Answer<CompletableFuture<Void>>) invocationOnMock -> {
                    EventData data = invocationOnMock.getArgumentAt(0, EventData.class);
                    String key = invocationOnMock.getArgumentAt(1, String.class);
                    Integer intKey = Integer.valueOf(key);
                    receivedData.get(systemName).get(streamName).get(intKey % 2).add(data);
                    return new CompletableFuture<>();
                  });
      } catch (Exception e) {
        Assert.fail("Failed to create create mock methods for EventHubClient");
      }
    }

    @Override
    public void init() {
      initiated = true;
    }

    @Override
    public EventHubClient getEventHubClient() {
      if (!initiated) {
        Assert.fail("Should have called init() on EventHubClient before getEventHubClient()");
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

    private class MockFuture extends CompletableFuture<EventHubRuntimeInformation> {
      EventHubRuntimeInformation runtimeInformation;

      MockFuture(EventHubRuntimeInformation runtimeInformation) {
        this.runtimeInformation = runtimeInformation;
      }

      @Override
      public EventHubRuntimeInformation get(long timeout, TimeUnit unit) {
        return runtimeInformation;
      }
    }

  }

}
