package org.apache.samza.system.eventhub.consumer;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import org.apache.samza.system.eventhub.EventHubClientFactory;
import org.apache.samza.system.eventhub.EventHubConfig;

import java.util.List;
import java.util.Map;

class MockEventHubEntityConnectionFactory extends EventHubEntityConnectionFactory {

  private final Map<String, Map<Integer, List<EventData>>> _eventData;

  MockEventHubEntityConnectionFactory(Map<String, Map<Integer, List<EventData>>> eventData) {
    _eventData = eventData;
  }

  @Override
  EventHubEntityConnection createConnection(String namespace, String entityPath, String sasKeyName, String sasKey,
                                            String consumerName, EventHubConfig eventHubConfig) {
    return new MockEventHubEntityConnection(entityPath, _eventData.get(entityPath));
  }

  private class MockEventHubEntityConnection extends EventHubEntityConnection {
    private final Map<Integer, List<EventData>> _eventData;

    MockEventHubEntityConnection(String entity, Map<Integer, List<EventData>> eventData) {
      super(null, entity, null, null, null, null);
      assert eventData != null : "No event data found for entity:" + entity;
      _eventData = eventData;
    }

    @Override
    void connectAndStart() {
      _offsets.keySet().forEach(partitionId -> {
        List<EventData> events = _eventData.get(partitionId);
        PartitionReceiveHandler partitionHandler = _handlers.get(partitionId);
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
