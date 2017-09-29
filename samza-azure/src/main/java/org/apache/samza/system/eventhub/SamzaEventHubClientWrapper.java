package org.apache.samza.system.eventhub;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ClientConstants;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SamzaEventHubClientWrapper implements EventHubClientWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaEventHubClientWrapper.class.getName());
  private static final String EVENTHUB_REMOTE_HOST_FORMAT = "%s.servicebus.windows.net";

  private EventHubClient _eventHubClient;

  private final String _eventHubNamespace;
  private final String _entityPath;
  private final String _sasKeyName;
  private final String _sasKey;

  public SamzaEventHubClientWrapper(String eventHubNamespace, String entityPath, String sasKeyName, String sasKey) {
    _eventHubNamespace = eventHubNamespace;
    _entityPath = entityPath;
    _sasKeyName = sasKeyName;
    _sasKey = sasKey;
  }

  public void init() {
    String remoteHost = String.format(EVENTHUB_REMOTE_HOST_FORMAT, _eventHubNamespace);
    try {
      ConnectionStringBuilder connectionStringBuilder =
              new ConnectionStringBuilder(_eventHubNamespace, _entityPath, _sasKeyName, _sasKey);

      _eventHubClient = EventHubClient.createFromConnectionStringSync(connectionStringBuilder.toString());
    } catch (IOException ioe) {
      throw new IllegalStateException(
              "Failed to connect to remote host " + remoteHost + ":" + ClientConstants.AMQPS_PORT, ioe);
    } catch (ServiceBusException e) {
      String msg = String.format("Creation of event hub client failed for eventHub %s %s %s %s with exception",
              _entityPath, _eventHubNamespace, _sasKeyName, _sasKey);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    }
  }

  public EventHubClient getEventHubClient() {
    return _eventHubClient;
  }


  public void close(long timeoutMS) {
    if (timeoutMS <= 0) {
      try {
        _eventHubClient.closeSync();
      } catch (ServiceBusException e) {
        LOG.warn("Closing the event hub client failed ", e);
      }
    } else {
      CompletableFuture<Void> future = _eventHubClient.close();
      try {
        future.get(timeoutMS, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        LOG.warn("Closing the event hub client failed ", e);
      }
    }

  }

}
