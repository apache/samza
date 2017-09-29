package org.apache.samza.system.eventhub.consumer;

import org.apache.samza.system.eventhub.EventHubConfig;

public class EventHubEntityConnectionFactory {
  EventHubEntityConnection createConnection(String namespace, String entityPath, String sasKeyName, String sasKey,
                                            String consumerName, EventHubConfig config) {
    return new EventHubEntityConnection(namespace, entityPath, sasKeyName, sasKey, consumerName, config);
  }
}
