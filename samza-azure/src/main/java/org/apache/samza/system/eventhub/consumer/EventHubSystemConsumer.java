package org.apache.samza.system.eventhub.consumer;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.StringUtil;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.EventDataWrapper;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.metrics.SamzaHistogram;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Implementation of a system consumer for EventHubs. For each system stream
 * partition, it registers a handler with the EventHubsClient which constantly
 * push data into a block queue. This class extends the BlockingEnvelopeMap
 * provided by samza-api to to simplify the logic around those blocking queues.
 * <p>
 * A high level architecture:
 * <p>
 * ┌───────────────────────────────────────────────┐
 * │ EventHubsClient                               │
 * │                                               │
 * │   ┌───────────────────────────────────────┐   │        ┌─────────────────────┐
 * │   │                                       │   │        │                     │
 * │   │       PartitionReceiveHandler_1       │───┼───────▶│ SSP1-BlockingQueue  ├──────┐
 * │   │                                       │   │        │                     │      │
 * │   └───────────────────────────────────────┘   │        └─────────────────────┘      │
 * │                                               │                                     │
 * │   ┌───────────────────────────────────────┐   │        ┌─────────────────────┐      │
 * │   │                                       │   │        │                     │      │
 * │   │       PartitionReceiveHandler_2       │───┼───────▶│ SSP2-BlockingQueue  ├──────┤        ┌──────────────────────────┐
 * │   │                                       │   │        │                     │      ├───────▶│                          │
 * │   └───────────────────────────────────────┘   │        └─────────────────────┘      └───────▶│  SystemConsumer.poll()   │
 * │                                               │                                     ┌───────▶│                          │
 * │                                               │                                     │        └──────────────────────────┘
 * │                      ...                      │                  ...                │
 * │                                               │                                     │
 * │                                               │                                     │
 * │   ┌───────────────────────────────────────┐   │        ┌─────────────────────┐      │
 * │   │                                       │   │        │                     │      │
 * │   │       PartitionReceiveHandler_N       │───┼───────▶│ SSPN-BlockingQueue  ├──────┘
 * │   │                                       │   │        │                     │
 * │   └───────────────────────────────────────┘   │        └─────────────────────┘
 * │                                               │
 * │                                               │
 * └───────────────────────────────────────────────┘
 */
public class EventHubSystemConsumer extends BlockingEnvelopeMap {

  public static final String START_OF_STREAM = PartitionReceiver.START_OF_STREAM; // -1
  public static final String END_OF_STREAM = "-2";
  public static final String AGGREGATE = "aggregate";
  public static final String EVENT_READ_RATE = "eventReadRate";
  public static final String EVENT_BYTE_READ_RATE = "eventByteReadRate";
  public static final String READ_LATENCY = "readLatency";
  public static final String READ_ERRORS = "readErrors";
  private static final Logger LOG = LoggerFactory.getLogger(EventHubSystemConsumer.class);
  private static final int MAX_EVENT_COUNT_PER_PARTITION_POLL = 50;
  private static final int BLOCKING_QUEUE_SIZE = 100;
  private static Counter _aggEventReadRate = null;
  private static Counter _aggEventByteReadRate = null;
  private static SamzaHistogram _aggReadLatency = null;
  private static Counter _aggReadErrors = null;
  private final Map<String, EventHubEntityConnection> _connections = new HashMap<>();
  private final Map<String, Serde<byte[]>> _serdes = new HashMap<>();
  private final EventHubConfig _config;
  private Map<String, Counter> _eventReadRates;
  private Map<String, Counter> _eventByteReadRates;
  private Map<String, SamzaHistogram> _readLatencies;
  private Map<String, Counter> _readErrors;

  public EventHubSystemConsumer(EventHubConfig config, EventHubEntityConnectionFactory connectionFactory,
                                MetricsRegistry registry) {
    super(registry, System::currentTimeMillis);

    _config = config;
    List<String> streamList = config.getStreamList();
    streamList.forEach(stream -> {
      _connections.put(stream, connectionFactory.createConnection(
              config.getStreamNamespace(stream), config.getStreamEntityPath(stream),
              config.getStreamSasKeyName(stream), config.getStreamSasToken(stream),
              config.getStreamConsumerGroup(stream)));
      _serdes.put(stream, config.getSerde(stream).orElse(null));
    });
    _eventReadRates = streamList.stream()
            .collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, EVENT_READ_RATE)));
    _eventByteReadRates = streamList.stream()
            .collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, EVENT_BYTE_READ_RATE)));
    _readLatencies = streamList.stream()
            .collect(Collectors.toMap(Function.identity(), x -> new SamzaHistogram(registry, x, READ_LATENCY)));
    _readErrors =
            streamList.stream().collect(Collectors.toMap(Function.identity(), x -> registry.newCounter(x, READ_ERRORS)));

    // Locking to ensure that these aggregated metrics will be created only once across multiple system consumers.
    synchronized (AGGREGATE) {
      if (_aggEventReadRate == null) {
        _aggEventReadRate = registry.newCounter(AGGREGATE, EVENT_READ_RATE);
        _aggEventByteReadRate = registry.newCounter(AGGREGATE, EVENT_BYTE_READ_RATE);
        _aggReadLatency = new SamzaHistogram(registry, AGGREGATE, READ_LATENCY);
        _aggReadErrors = registry.newCounter(AGGREGATE, READ_ERRORS);
      }
    }
  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) {
    super.register(systemStreamPartition, offset);
    String stream = systemStreamPartition.getStream();
    EventHubEntityConnection connection = _connections.get(stream);
    if (connection == null) {
      throw new SamzaException("No EventHub connection for " + stream);
    }

    if (StringUtil.isNullOrWhiteSpace(offset)) {
      switch (_config.getStartPosition(systemStreamPartition.getStream())) {
        case EARLIEST:
          offset = START_OF_STREAM;
          break;
        case LATEST:
          offset = END_OF_STREAM;
          break;
        default:
          throw new SamzaException(
                  "Unknown starting position config " + _config.getStartPosition(systemStreamPartition.getStream()));
      }
    }
    connection.addPartition(systemStreamPartition.getPartition().getPartitionId(), offset,
            new PartitionReceiverHandlerImpl(systemStreamPartition, _eventReadRates.get(stream),
                    _eventByteReadRates.get(stream), _readLatencies.get(stream), _readErrors.get(stream),
                    _serdes.get(stream)));
  }

  @Override
  public void start() {
    _connections.values().forEach(EventHubEntityConnection::connectAndStart);
  }

  @Override
  public void stop() {
    _connections.values().forEach(EventHubEntityConnection::stop);
  }

  private class PartitionReceiverHandlerImpl extends PartitionReceiveHandler {

    private final Counter _eventReadRate;
    private final Counter _eventByteReadRate;
    private final SamzaHistogram _readLatency;
    private final Counter _errors;
    private final Serde<byte[]> _serde;
    SystemStreamPartition _ssp;

    PartitionReceiverHandlerImpl(SystemStreamPartition ssp, Counter eventReadRate, Counter eventByteReadRate,
                                 SamzaHistogram readLatency, Counter readErrors, Serde<byte[]> serde) {
      super(MAX_EVENT_COUNT_PER_PARTITION_POLL);
      _ssp = ssp;
      _eventReadRate = eventReadRate;
      _eventByteReadRate = eventByteReadRate;
      _readLatency = readLatency;
      _errors = readErrors;
      _serde = serde;
    }

    @Override
    public void onReceive(Iterable<EventData> events) {
      if (events != null) {

        events.forEach(event -> {
          byte[] decryptedBody = event.getBody();
          if (_serde != null) {
            decryptedBody = _serde.fromBytes(decryptedBody);
          }
          EventDataWrapper wrappedEvent = new EventDataWrapper(event, decryptedBody);
          try {
            updateMetrics(event);
            // note that the partition key can be null
            put(_ssp, new IncomingMessageEnvelope(_ssp, event.getSystemProperties().getOffset(),
                    event.getSystemProperties().getPartitionKey(), wrappedEvent));
          } catch (Exception e) {
            String msg = String.format("Exception while adding the event from ssp %s to dispatch queue.", _ssp);
            LOG.error(msg, e);
            throw new SamzaException(msg, e);
          }
        });
      }
    }

    private void updateMetrics(EventData event) {
      _eventReadRate.inc();
      _aggEventReadRate.inc();
      _eventByteReadRate.inc(event.getBodyLength());
      _aggEventByteReadRate.inc(event.getBodyLength());
      long latencyMs = Duration.between(Instant.now(), event.getSystemProperties().getEnqueuedTime()).toMillis();
      _readLatency.update(latencyMs);
      _aggReadLatency.update(latencyMs);
    }

    @Override
    public void onError(Throwable throwable) {
      // TODO error handling
      _errors.inc();
      _aggReadErrors.inc();
      LOG.error(String.format("Received error from event hub connection (ssp=%s): ", _ssp), throwable);
    }
  }
}
