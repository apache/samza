package org.apache.samza.system.eventhub;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionSender;
import com.microsoft.azure.servicebus.ClientConstants;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EventHubClientWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(EventHubClientWrapper.class.getName());
  private final PartitioningMethod _partitioningMethod;
  private final int _numPartitions;
  private EventHubClient _eventHubClient;
  private Map<Integer, PartitionSender> _partitionSenders = new HashMap<>();

  public EventHubClientWrapper(PartitioningMethod partitioningMethod, int numPartitions,
                               String eventHubNamespace, String entityPath, String sasKeyName, String sasToken) {
    String remoteHost = String.format("%s.servicebus.windows.net", eventHubNamespace);
    _partitioningMethod = partitioningMethod;
    _numPartitions = numPartitions;
    try {
      // Create a event hub connection string pointing to localhost
      ConnectionStringBuilder connectionStringBuilder =
              new ConnectionStringBuilder(eventHubNamespace, entityPath, sasKeyName, sasToken);

      _eventHubClient = EventHubClient.createFromConnectionStringSync(connectionStringBuilder.toString());
    } catch (IOException ioe) {
      throw new IllegalStateException(
              "Failed to connect to remote host " + remoteHost + ":" + ClientConstants.AMQPS_PORT, ioe);
    } catch (ServiceBusException e) {
      String msg = String.format("Creation of event hub client failed for eventHub %s %s %s %s %s %s with exception",
              entityPath, partitioningMethod, numPartitions, eventHubNamespace, sasKeyName, sasToken);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    }
  }

  public EventHubClient getEventHubClient() {
    return _eventHubClient;
  }

  public void closeSync() {
    _partitionSenders.entrySet().forEach(x -> {
      try {
        x.getValue().closeSync();
      } catch (ServiceBusException e) {
        LOG.warn("Closing the partition sender failed for partition " + x.getKey(), e);
      }
    });

    try {
      _eventHubClient.closeSync();
    } catch (ServiceBusException e) {
      LOG.warn("Closing the event hub client failed ", e);
    }
  }

  /**
   * Timed connection close.
   *
   * @param timeoutMS Time in Miliseconds to wait for individual components (partition senders, event hub client and
   *                  tunnel in order) to shutdown before moving to the next stage.
   *                  For example a timeoutMS of 30000, will result in a wait of max 30secs for a successful close of
   *                  all partition senders followed by a max 30secs wait for a successful close of eventhub client and
   *                  then another max 30secs wait for successful tunnel close. Tunnel close failure/timeout will result
   *                  in RuntimeException.
   */

  public void closeSync(long timeoutMS) {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    _partitionSenders.entrySet().forEach(x -> futures.add(x.getValue().close()));
    CompletableFuture<Void> future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    try {
      future.get(timeoutMS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.warn("Closing the partition sender failed ", e);
    }

    future = _eventHubClient.close();
    try {
      future.get(timeoutMS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.warn("Closing the event hub client failed ", e);
    }
  }

  public CompletableFuture<Void> send(EventData eventData, Object partitionKey) {
    if (_partitioningMethod == PartitioningMethod.EVENT_HUB_HASHING) {
      return _eventHubClient.send(eventData, convertPartitionKeyToString(partitionKey));
    } else if (_partitioningMethod == PartitioningMethod.PARTITION_KEY_AS_PARTITION) {
      if (!(partitionKey instanceof Integer)) {
        String msg = "Partition key should be of type Integer";
        LOG.error(msg);
        throw new SamzaException(msg);
      }

      PartitionSender sender = getPartitionSender((int) partitionKey);
      return sender.send(eventData);
    } else {
      throw new SamzaException("Unknown partitioning method " + _partitioningMethod);
    }
  }

  private String convertPartitionKeyToString(Object partitionKey) {
    if (partitionKey instanceof String) {
      return (String) partitionKey;
    } else if (partitionKey instanceof Integer) {
      return String.valueOf(partitionKey);
    } else if (partitionKey instanceof byte[]) {
      return new String((byte[]) partitionKey, Charset.defaultCharset());
    } else {
      throw new SamzaException("Unsupported key type: " + partitionKey.getClass().toString());
    }
  }

  private PartitionSender getPartitionSender(int partition) {
    if (!_partitionSenders.containsKey(partition)) {
      try {
        PartitionSender partitionSender =
                _eventHubClient.createPartitionSenderSync(String.valueOf(partition % _numPartitions));
        _partitionSenders.put(partition, partitionSender);
      } catch (ServiceBusException e) {
        String msg = "Creation of partition sender failed with exception";
        LOG.error(msg, e);
        throw new SamzaException(msg, e);
      }
    }

    return _partitionSenders.get(partition);
  }

  public enum PartitioningMethod {
    EVENT_HUB_HASHING,
    PARTITION_KEY_AS_PARTITION,
  }
}
