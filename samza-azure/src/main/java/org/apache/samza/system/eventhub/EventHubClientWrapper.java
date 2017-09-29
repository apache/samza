package org.apache.samza.system.eventhub;

import com.microsoft.azure.eventhubs.EventHubClient;

public interface EventHubClientWrapper {
  /**
   * Initiate the connection to EventHub
   */
  void init();

  /**
   * Returns the EventHubClient instance of the wrapper so its methods can be invoked directly
   *
   * @return EventHub client instance of the wrapper
   */
  EventHubClient getEventHubClient();

  /**
   * Timed synchronous connection close to the EventHub.
   *
   * @param timeoutMS
   *            Time in Milliseconds to wait for individual components to
   *            shutdown before moving to the next stage.
   */
  void close(long timeoutMS);
}