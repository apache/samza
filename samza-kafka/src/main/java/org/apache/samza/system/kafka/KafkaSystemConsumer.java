
/*
 *
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
 *
 */

package org.apache.samza.system.kafka;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import kafka.common.TopicAndPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.startpoint.Startpoint;
import org.apache.samza.startpoint.StartpointOldest;
import org.apache.samza.startpoint.StartpointSpecific;
import org.apache.samza.startpoint.StartpointTimestamp;
import org.apache.samza.startpoint.StartpointUpcoming;
import org.apache.samza.startpoint.StartpointVisitor;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class KafkaSystemConsumer<K, V> extends BlockingEnvelopeMap implements SystemConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSystemConsumer.class);

  private static final long FETCH_THRESHOLD = 10000;
  private static final long FETCH_THRESHOLD_BYTES = -1L;

  protected final Consumer<K, V> kafkaConsumer;
  protected final String systemName;
  protected final String clientId;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Config config;
  private final boolean fetchThresholdBytesEnabled;
  private final KafkaSystemConsumerMetrics metrics;
  private final KafkaStartpointRegistrationHandler kafkaStartpointRegistrationHandler;

  // This sink is used to transfer the messages from the proxy/consumer to the BlockingEnvelopeMap.
  final KafkaConsumerMessageSink messageSink;

  // This proxy contains a separate thread, which reads kafka messages (with consumer.poll()) and populates
  // BlockingEnvelopMap's buffers.
  private final KafkaConsumerProxy proxy;

  // Holds mapping from {@code TopicPartition} to registered {@code Startpoint}. This will be used in the start().
  final Map<TopicPartition, Startpoint> topicPartitionToStartpointMap = new HashMap<>();

  long perPartitionFetchThreshold;
  long perPartitionFetchThresholdBytes;

  /**
   * Create a KafkaSystemConsumer for the provided {@code systemName}
   * @param kafkaConsumer kafka Consumer object to be used by this system consumer
   * @param systemName system name for which we create the consumer
   * @param config application config
   * @param clientId clientId from the kafka consumer
   * @param kafkaConsumerProxyFactory factory for creating a KafkaConsumerProxy to use in this consumer
   * @param metrics metrics for this KafkaSystemConsumer
   * @param clock system clock
   */
  public KafkaSystemConsumer(Consumer<K, V> kafkaConsumer, String systemName, Config config, String clientId,
      KafkaConsumerProxyFactory<K, V> kafkaConsumerProxyFactory, KafkaSystemConsumerMetrics metrics, Clock clock) {
    super(metrics.registry(), clock, metrics.getClass().getName());

    this.kafkaConsumer = kafkaConsumer;
    this.clientId = clientId;
    this.systemName = systemName;
    this.config = config;
    this.metrics = metrics;

    fetchThresholdBytesEnabled = new KafkaConfig(config).isConsumerFetchThresholdBytesEnabled(systemName);

    // create a sink for passing the messages between the proxy and the consumer
    this.messageSink = new KafkaConsumerMessageSink();

    // Create the proxy to do the actual message reading.
    proxy = kafkaConsumerProxyFactory.create(this.messageSink);
    LOG.info("{}: Created proxy {} ", this, proxy);
    this.kafkaStartpointRegistrationHandler = new KafkaStartpointRegistrationHandler(kafkaConsumer, proxy);
  }

  @VisibleForTesting
  KafkaSystemConsumer(Consumer<K, V> kafkaConsumer, String systemName, Config config, String clientId,
      KafkaConsumerProxy<K, V> kafkaConsumerProxy, KafkaSystemConsumerMetrics metrics, Clock clock, KafkaStartpointRegistrationHandler kafkaStartpointRegistrationHandler) {
    this.kafkaConsumer = kafkaConsumer;
    this.clientId = clientId;
    this.systemName = systemName;
    this.config = config;
    this.metrics = metrics;
    this.proxy = kafkaConsumerProxy;
    this.kafkaStartpointRegistrationHandler = kafkaStartpointRegistrationHandler;
    this.messageSink = new KafkaConsumerMessageSink();
    fetchThresholdBytesEnabled = new KafkaConfig(config).isConsumerFetchThresholdBytesEnabled(systemName);
  }

  /**
   * Create internal kafka consumer object, which will be used in the Proxy.
   * @param <K> key type for the consumer
   * @param <V> value type for the consumer
   * @param systemName system name for which we create the consumer
   * @param kafkaConsumerConfig config object for Kafka's KafkaConsumer
   * @return KafkaConsumer newly created kafka consumer object
   */
  public static <K, V> KafkaConsumer<K, V> createKafkaConsumerImpl(String systemName, HashMap<String, Object> kafkaConsumerConfig) {
    LOG.info("Instantiating KafkaConsumer for systemName {} with properties {}", systemName, kafkaConsumerConfig);
    return new KafkaConsumer<>(kafkaConsumerConfig);
  }

  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      LOG.warn("{}: Attempting to start the consumer for the second (or more) time.", this);
      return;
    }
    if (stopped.get()) {
      LOG.error("{}: Attempting to start a stopped consumer", this);
      return;
    }
    // initialize the subscriptions for all the registered TopicPartitions
    startSubscription();
    // needs to be called after all the registrations are completed
    setFetchThresholds();

    startConsumer();
    LOG.info("{}: Consumer started", this);
  }

  private void startSubscription() {
    //subscribe to all the registered TopicPartitions
    Set<TopicPartition> registeredTopicPartitions = new HashSet<>(topicPartitionToStartpointMap.keySet());
    LOG.info("{}: Consumer subscribes to {}", this, registeredTopicPartitions);
    try {
      synchronized (kafkaConsumer) {
        // we are using assign (and not subscribe), so we need to specify both topic and partition
        kafkaConsumer.assign(registeredTopicPartitions);
      }
    } catch (Exception e) {
      throw new SamzaException("Consumer subscription failed for " + this, e);
    }
  }

  /**
   * Set the offsets to start from.
   * Register the TopicPartitions with the proxy.
   * Start the proxy.
   */
  void startConsumer() {
    // set the offset for each TopicPartition
    if (topicPartitionToStartpointMap.size() <= 0) {
      LOG.error ("{}: Consumer is not subscribed to any SSPs", this);
    }

    topicPartitionToStartpointMap.forEach((topicPartition, startpoint) -> {
      Partition partition = new Partition(topicPartition.partition());
      SystemStreamPartition systemStreamPartition = new SystemStreamPartition(systemName, topicPartition.topic(), partition);
      startpoint.apply(systemStreamPartition, kafkaStartpointRegistrationHandler);
    });

    // start the proxy thread
    if (proxy != null && !proxy.isRunning()) {
      LOG.info("{}: Starting proxy {}", this, proxy);
      proxy.start();
    }
  }

  private void setFetchThresholds() {
    // get the thresholds, and set defaults if not defined.
    KafkaConfig kafkaConfig = new KafkaConfig(config);

    Option<String> fetchThresholdOption = kafkaConfig.getConsumerFetchThreshold(systemName);
    long fetchThreshold = FETCH_THRESHOLD;
    if (fetchThresholdOption.isDefined()) {
      fetchThreshold = Long.valueOf(fetchThresholdOption.get());
    }

    Option<String> fetchThresholdBytesOption = kafkaConfig.getConsumerFetchThresholdBytes(systemName);
    long fetchThresholdBytes = FETCH_THRESHOLD_BYTES;
    if (fetchThresholdBytesOption.isDefined()) {
      fetchThresholdBytes = Long.valueOf(fetchThresholdBytesOption.get());
    }

    int numPartitions = topicPartitionToStartpointMap.size();

    if (numPartitions > 0) {
      perPartitionFetchThreshold = fetchThreshold / numPartitions;
      if (fetchThresholdBytesEnabled) {
        // currently this feature cannot be enabled, because we do not have the size of the messages available.
        // messages get double buffered, hence divide by 2
        perPartitionFetchThresholdBytes = (fetchThresholdBytes / 2) / numPartitions;
      }
    }
    LOG.info("{}: fetchThresholdBytes = {}; fetchThreshold={}; numPartitions={}, perPartitionFetchThreshold={}, perPartitionFetchThresholdBytes(0 if disabled)={}",
        this, fetchThresholdBytes, fetchThreshold, numPartitions, perPartitionFetchThreshold, perPartitionFetchThresholdBytes);
  }

  @Override
  public void stop() {
    if (!stopped.compareAndSet(false, true)) {
      LOG.warn("{}: Attempting to stop stopped consumer.", this);
      return;
    }

    LOG.info("{}: Stopping Samza kafkaConsumer ", this);

    // stop the proxy (with 1 minute timeout)
    if (proxy != null) {
      LOG.info("{}: Stopping proxy {}", this, proxy);
      proxy.stop(TimeUnit.SECONDS.toMillis(60));
    }

    try {
      synchronized (kafkaConsumer) {
        LOG.info("{}: Closing kafkaSystemConsumer {}", this, kafkaConsumer);
        kafkaConsumer.close();
      }
    } catch (Exception e) {
      LOG.warn("{}: Failed to stop KafkaSystemConsumer.", this, e);
    }
  }

  /**
   * record the ssp and the offset. Do not submit it to the consumer yet.
   * @param systemStreamPartition ssp to register
   * @param offset offset to register with
   */
  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) {
    register(systemStreamPartition, new StartpointSpecific(offset));
  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, Startpoint startpoint) {
    if (started.get()) {
      String exceptionMessage = String.format("KafkaSystemConsumer: %s had started. Registration of ssp: %s, startpoint: %s failed.", this, systemStreamPartition, startpoint);
      throw new SamzaException(exceptionMessage);
    }

    if (!Objects.equals(systemStreamPartition.getSystem(), systemName)) {
      LOG.warn("{}: ignoring SSP {}, because this consumer's system doesn't match.", this, systemStreamPartition);
      return;
    }

    LOG.debug("Registering the ssp: {}, startpoint: {} with the consumer.", systemStreamPartition, startpoint);

    super.register(systemStreamPartition, startpoint);

    TopicPartition topicPartition = toTopicPartition(systemStreamPartition);
    topicPartitionToStartpointMap.put(topicPartition, startpoint);
    metrics.registerTopicAndPartition(toTopicAndPartition(topicPartition));
  }

  @Override
  public String toString() {
    return String.format("%s:%s", systemName, clientId);
  }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
      Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {

    // check if the proxy is running
    if (!proxy.isRunning()) {
      LOG.info("{}: KafkaConsumerProxy is not running. Stopping the consumer.", this);
      stop();
      String message = String.format("%s: KafkaConsumerProxy has stopped.", this);
      throw new SamzaException(message, proxy.getFailureCause());
    }

    return super.poll(systemStreamPartitions, timeout);
  }

  static TopicAndPartition toTopicAndPartition(TopicPartition tp) {
    return new TopicAndPartition(tp.topic(), tp.partition());
  }

  static TopicPartition toTopicPartition(SystemStreamPartition ssp) {
    return new TopicPartition(ssp.getStream(), ssp.getPartition().getPartitionId());
  }

  /**
   * return system name for this consumer
   * @return system name
   */
  public String getSystemName() {
    return systemName;
  }

  @VisibleForTesting
  static class KafkaStartpointRegistrationHandler implements StartpointVisitor {

    private final KafkaConsumerProxy proxy;
    private final Consumer kafkaConsumer;

    KafkaStartpointRegistrationHandler(Consumer consumer, KafkaConsumerProxy proxy) {
      this.proxy = proxy;
      this.kafkaConsumer = consumer;
    }

    @Override
    public void visit(SystemStreamPartition systemStreamPartition, StartpointSpecific startpointSpecific) {
      TopicPartition topicPartition = toTopicPartition(systemStreamPartition);
      long offsetInStartpoint = Long.parseLong(startpointSpecific.getSpecificOffset());
      LOG.info("Updating the consumer fetch offsets of topic partition: {} to {}.", topicPartition, offsetInStartpoint);

      // KafkaConsumer is not thread-safe.
      synchronized (kafkaConsumer) {
        kafkaConsumer.seek(topicPartition, offsetInStartpoint);

        // add the partition to the proxy
        proxy.addTopicPartition(systemStreamPartition, kafkaConsumer.position(topicPartition));
      }
    }

    @Override
    public void visit(SystemStreamPartition systemStreamPartition, StartpointTimestamp startpointTimestamp) {
      Long timestampInStartpoint = startpointTimestamp.getTimestampOffset();
      TopicPartition topicPartition = toTopicPartition(systemStreamPartition);
      Map<TopicPartition, Long> topicPartitionsToTimeStamps = ImmutableMap.of(topicPartition, timestampInStartpoint);

      // Look up the offset by timestamp.
      LOG.info("Looking up the offsets of the topic partition: {} by timestamp: {}.", topicPartition, timestampInStartpoint);
      Map<TopicPartition, OffsetAndTimestamp> topicPartitionToOffsetTimestamps = new HashMap<>();
      synchronized (kafkaConsumer) {
        topicPartitionToOffsetTimestamps = kafkaConsumer.offsetsForTimes(topicPartitionsToTimeStamps);
      }

      // If the timestamp does not exist for the partition, then seek the consumer to end.
      if (topicPartitionToOffsetTimestamps.get(topicPartition) == null) {
        LOG.info("Timestamp does not exist for partition: {}. Seeking the kafka consumer to the end offset.", topicPartition);

        // KafkaConsumer is not thread-safe.
        synchronized (kafkaConsumer) {
          kafkaConsumer.seekToEnd(ImmutableList.of(topicPartition));

          // add the partition to the proxy
          proxy.addTopicPartition(systemStreamPartition, kafkaConsumer.position(topicPartition));
        }
      } else {

        // KafkaConsumer is not thread-safe.
        synchronized (kafkaConsumer) {
          // Update the consumer fetch offsets.
          OffsetAndTimestamp offsetAndTimeStamp = topicPartitionToOffsetTimestamps.get(topicPartition);
          LOG.info("Updating the consumer fetch offsets of the topic partition: {} to {}.", topicPartition, offsetAndTimeStamp.offset());
          kafkaConsumer.seek(topicPartition, offsetAndTimeStamp.offset());

          // add the partition to the proxy
          proxy.addTopicPartition(systemStreamPartition, kafkaConsumer.position(topicPartition));
        }
      }
    }

    @Override
    public void visit(SystemStreamPartition systemStreamPartition, StartpointOldest startpointOldest) {
      TopicPartition topicPartition = toTopicPartition(systemStreamPartition);
      Collection<TopicPartition> topicPartitions = ImmutableList.of(topicPartition);
      LOG.info("Seeking the kafka consumer to the first offset for the topic partition: {}.", topicPartitions);

      // KafkaConsumer is not thread-safe.
      synchronized (kafkaConsumer) {
        kafkaConsumer.seekToBeginning(topicPartitions);
        // add the partition to the proxy
        proxy.addTopicPartition(systemStreamPartition, kafkaConsumer.position(topicPartition));
      }
    }

    @Override
    public void visit(SystemStreamPartition systemStreamPartition, StartpointUpcoming startpointUpcoming) {
      TopicPartition topicPartition = toTopicPartition(systemStreamPartition);
      Collection<TopicPartition> topicPartitions = ImmutableList.of(topicPartition);
      LOG.info("Seeking the kafka consumer to the end offset for the topic partition: {}.", topicPartitions);

      // KafkaConsumer is not thread-safe.
      synchronized (kafkaConsumer) {
        kafkaConsumer.seekToEnd(topicPartitions);
        // add the partition to the proxy
        proxy.addTopicPartition(systemStreamPartition, kafkaConsumer.position(topicPartition));
      }
    }
  }

  public class KafkaConsumerMessageSink {

    public void setIsAtHighWatermark(SystemStreamPartition ssp, boolean isAtHighWatermark) {
      setIsAtHead(ssp, isAtHighWatermark);
    }

    boolean needsMoreMessages(SystemStreamPartition ssp) {
      LOG.debug("{}: needsMoreMessages from following SSP: {}. fetchLimitByBytes enabled={}; messagesSizeInQueue={};"
              + "(limit={}); messagesNumInQueue={}(limit={};", this, ssp, fetchThresholdBytesEnabled,
          getMessagesSizeInQueue(ssp), perPartitionFetchThresholdBytes, getNumMessagesInQueue(ssp),
          perPartitionFetchThreshold);

      if (fetchThresholdBytesEnabled) {
        return getMessagesSizeInQueue(ssp) < perPartitionFetchThresholdBytes;
      } else {
        return getNumMessagesInQueue(ssp) < perPartitionFetchThreshold;
      }
    }

    void addMessage(SystemStreamPartition ssp, IncomingMessageEnvelope envelope) {
      LOG.trace("{}: Incoming message ssp = {}: envelope = {}.", this, ssp, envelope);

      try {
        put(ssp, envelope);
      } catch (InterruptedException e) {
        throw new SamzaException(
            String.format("%s: Consumer was interrupted while trying to add message with offset %s for ssp %s", this,
                envelope.getOffset(), ssp));
      }
    }
  }
}
