package org.apache.samza.system.eventhub.consumer;

public class EventHubEntityConnectionFactory {
  EventHubEntityConnection createConnection(String namespace, String entityPath, String sasKeyName, String sasKey,
                                            String consumerName) {
    return new EventHubEntityConnection(namespace, entityPath, sasKeyName, sasKey, consumerName);
  }
}
