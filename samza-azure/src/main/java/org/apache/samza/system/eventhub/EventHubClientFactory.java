package org.apache.samza.system.eventhub;

public class EventHubClientFactory {
  public EventHubClientWrapper getEventHubClient(String eventHubNamespace, String entityPath, String sasKeyName,
                                                      String sasToken, EventHubConfig config) {
    return new SamzaEventHubClientWrapper(eventHubNamespace, entityPath, sasKeyName, sasToken);
  }
}
