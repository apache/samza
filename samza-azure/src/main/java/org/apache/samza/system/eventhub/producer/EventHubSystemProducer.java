package org.apache.samza.system.eventhub.producer;

import com.microsoft.azure.eventhubs.EventData;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.eventhub.EventHubClientWrapper;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.metrics.SamzaHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class EventHubSystemProducer implements SystemProducer {
  public static final String EVENT_SOURCE_TIMESTAMP = "event-source-timestamp";
  public static final String PRODUCE_TIMESTAMP = "produce-timestamp";
  public static final String AGGREGATE = "aggregate";
  public final static String CONFIG_PARTITIONING_METHOD = "partitioningMethod";
  public final static String DEFAULT_PARTITIONING_METHOD = EventHubClientWrapper.PartitioningMethod.EVENT_HUB_HASHING.toString();
  public static final String CONFIG_DESTINATION_NUM_PARTITION = "destinationPartitions";
  public static final String CONFIG_SEND_KEY_IN_EVENT_PROPERTIES = "sendKeyInEventProperties";
  private static final String EVENT_WRITE_RATE = "eventWriteRate";
  private static final String EVENT_BYTE_WRITE_RATE = "eventByteWriteRate";
  private static final String SEND_ERRORS = "sendErrors";
  private static final String SEND_LATENCY = "sendLatency";
  private static final String SEND_CALLBACK_LATENCY = "sendCallbackLatency";
  private static final Duration SHUTDOWN_WAIT_TIME = Duration.ofMinutes(1L);
  private static final Logger LOG = LoggerFactory.getLogger(EventHubSystemProducer.class.getName());
  private static Counter _aggEventWriteRate = null;
  private static Counter _aggEventByteWriteRate = null;
  private static SamzaHistogram _aggSendLatency = null;
  private static SamzaHistogram _aggSendCallbackLatency = null;
  private static Counter _aggSendErrors = null;
  private final int _destinationPartitions;
  private final boolean _sendKeyInEventProperties;
  private final EventHubConfig _eventHubsConfig;
  private final Config _config;
  private final EventHubClientWrapper.PartitioningMethod _partitioningMethod;
  private final String _systemName;
  private final MetricsRegistry _registry;
  private Throwable _sendExceptionOnCallback;
  private boolean _isStarted;
  private HashMap<String, Counter> _eventWriteRate = new HashMap<>();
  private HashMap<String, Counter> _eventByteWriteRate = new HashMap<>();
  private HashMap<String, SamzaHistogram> _sendLatency = new HashMap<>();
  private HashMap<String, SamzaHistogram> _sendCallbackLatency = new HashMap<>();
  private HashMap<String, Counter> _sendErrors = new HashMap<>();
  // Map of the system name to the event hub client.
  private Map<String, EventHubClientWrapper> _eventHubClients = new HashMap<>();

  private long _messageId;
  private Map<String, Serde<byte[]>> _serdes = new HashMap<>();

  private Map<Long, CompletableFuture<Void>> _pendingFutures = new ConcurrentHashMap<>();

  public EventHubSystemProducer(String systemName, Config config, MetricsRegistry registry) {
    _messageId = 0;
    _systemName = systemName;
    _registry = registry;
    _config = config;
    _partitioningMethod =
            EventHubClientWrapper.PartitioningMethod.valueOf(getConfigValue(config, CONFIG_PARTITIONING_METHOD, DEFAULT_PARTITIONING_METHOD));

    _eventHubsConfig = new EventHubConfig(config, systemName);
    _sendKeyInEventProperties =
            Boolean.parseBoolean(getConfigValue(config, CONFIG_SEND_KEY_IN_EVENT_PROPERTIES, "false"));

    // TODO this should be removed when we are able to find the number of partitions.
    _destinationPartitions = Integer.parseInt(getConfigValue(config, CONFIG_DESTINATION_NUM_PARTITION, "-1"));
  }

  private String getConfigValue(Config config, String configKey, String defaultValue) {
    String configValue = config.get(configKey, defaultValue);

    if (configValue == null) {
      throw new SamzaException(configKey + " is not configured.");
    }

    return configValue;
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting system producer.");
    for (String eventHub : _eventHubClients.keySet()) {
      _eventWriteRate.put(eventHub, _registry.newCounter(eventHub, EVENT_WRITE_RATE));
      _eventByteWriteRate.put(eventHub, _registry.newCounter(eventHub, EVENT_BYTE_WRITE_RATE));
      _sendLatency.put(eventHub, new SamzaHistogram(_registry, eventHub, SEND_LATENCY));
      _sendCallbackLatency.put(eventHub, new SamzaHistogram(_registry, eventHub, SEND_CALLBACK_LATENCY));
      _sendErrors.put(eventHub, _registry.newCounter(eventHub, SEND_ERRORS));
    }

    // Locking to ensure that these aggregated metrics will be created only once across multiple system consumers.
    synchronized (AGGREGATE) {
      if (_aggEventWriteRate == null) {
        _aggEventWriteRate = _registry.newCounter(AGGREGATE, EVENT_WRITE_RATE);
        _aggEventByteWriteRate = _registry.newCounter(AGGREGATE, EVENT_BYTE_WRITE_RATE);
        _aggSendLatency = new SamzaHistogram(_registry, AGGREGATE, SEND_LATENCY);
        _aggSendCallbackLatency = new SamzaHistogram(_registry, AGGREGATE, SEND_CALLBACK_LATENCY);
        _aggSendErrors = _registry.newCounter(AGGREGATE, SEND_ERRORS);
      }
    }

    _isStarted = true;
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping system producer.");
    _eventHubClients.values().forEach(ehClient -> ehClient.closeSync(SHUTDOWN_WAIT_TIME.toMillis()));
    _eventHubClients.clear();
  }

  @Override
  public synchronized void register(String streamName) {
    LOG.info("Trying to register {}.", streamName);
    if (_isStarted) {
      String msg = "Cannot register once the producer is started.";
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    String ehNamespace = _eventHubsConfig.getStreamNamespace(streamName);
    String ehName = _eventHubsConfig.getStreamEntityPath(streamName);

    EventHubClientWrapper ehClient =
            new EventHubClientWrapper(_partitioningMethod, _destinationPartitions, ehNamespace, ehName,
                    _eventHubsConfig.getStreamSasKeyName(streamName), _eventHubsConfig.getStreamSasToken(streamName));

    _eventHubClients.put(streamName, ehClient);
    _eventHubsConfig.getSerde(streamName).ifPresent(x -> _serdes.put(streamName, x));
  }

  @Override
  public synchronized void send(String destination, OutgoingMessageEnvelope envelope) {
    if (!_isStarted) {
      throw new SamzaException("Trying to call send before the producer is started.");
    }

    if (!_eventHubClients.containsKey(destination)) {
      String msg = String.format("Trying to send event to a destination {%s} that is not registered.", destination);
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    if (_sendExceptionOnCallback != null) {
      SamzaException e = new SamzaException(_sendExceptionOnCallback);
      _sendExceptionOnCallback = null;
      _pendingFutures.clear();
      LOG.error("One of the previous sends failed.");
      throw e;
    }

    EventData eventData = createEventData(destination, envelope);

    _eventWriteRate.get(destination).inc();
    _aggEventWriteRate.inc();
    _eventByteWriteRate.get(destination).inc(eventData.getBodyLength());
    _aggEventByteWriteRate.inc(eventData.getBodyLength());
    EventHubClientWrapper ehClient = _eventHubClients.get(destination);

    Instant startTime = Instant.now();

    CompletableFuture<Void> sendResult;
    sendResult = ehClient.send(eventData, envelope.getPartitionKey());

    Instant endTime = Instant.now();
    long latencyMs = Duration.between(startTime, endTime).toMillis();
    _sendLatency.get(destination).update(latencyMs);
    _aggSendLatency.update(latencyMs);

    long messageId = ++_messageId;

    // Rotate the messageIds
    if (messageId == Long.MAX_VALUE) {
      _messageId = 0;
    }

    _pendingFutures.put(messageId, sendResult);

    // Auto remove the future from the list when they are complete.
    sendResult.handle(((aVoid, throwable) -> {
      long callbackLatencyMs = Duration.between(endTime, Instant.now()).toMillis();
      _sendCallbackLatency.get(destination).update(callbackLatencyMs);
      _aggSendCallbackLatency.update(callbackLatencyMs);
      if (throwable != null) {
        _sendErrors.get(destination).inc();
        _aggSendErrors.inc();
        LOG.error("Send message to event hub: {} failed with exception: ", destination, throwable);
        _sendExceptionOnCallback = throwable;
      }
      _pendingFutures.remove(messageId);
      return aVoid;
    }));
  }

  private EventData createEventData(String streamName, OutgoingMessageEnvelope envelope) {
    Optional<Serde<byte[]>> serde = Optional.ofNullable(_serdes.getOrDefault(streamName, null));
    byte[] eventValue = (byte[]) envelope.getMessage();
    if (serde.isPresent()) {
      eventValue = serde.get().toBytes(eventValue);
    }

    EventData eventData = new EventData(eventValue);

    eventData.getProperties().put(PRODUCE_TIMESTAMP, Long.toString(System.currentTimeMillis()));

    if (_sendKeyInEventProperties) {
      String keyValue = "";
      if (envelope.getKey() != null) {
        keyValue = (envelope.getKey() instanceof byte[]) ? new String((byte[]) envelope.getKey())
                : envelope.getKey().toString();
      }
      eventData.getProperties().put("key", keyValue);
    }
    return eventData;
  }

  @Override
  public void flush(String source) {
    LOG.info("Trying to flush pending {} sends messages: {}", _pendingFutures.size(), _pendingFutures.keySet());
    // Wait till all the pending sends are complete.
    while (!_pendingFutures.isEmpty()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        String msg = "Flush failed with error";
        LOG.error(msg, e);
        throw new SamzaException(msg, e);
      }
    }

    if (_sendExceptionOnCallback != null) {
      String msg = "Sending one of the message failed during flush";
      Throwable throwable = _sendExceptionOnCallback;
      _sendExceptionOnCallback = null;
      LOG.error(msg, throwable);
      throw new SamzaException(msg, throwable);
    }

    LOG.info("Flush succeeded.");
  }

  Collection<CompletableFuture<Void>> getPendingFutures() {
    return _pendingFutures.values();
  }
}
