
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import kafka.common.TopicAndPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.KafkaConfig;
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

  private static final long FETCH_THRESHOLD = 50000;
  private static final long FETCH_THRESHOLD_BYTES = -1L;

  private final Consumer<K, V> kafkaConsumer;
  private final String systemName;
  private final KafkaSystemConsumerMetrics samzaConsumerMetrics;
  private final String clientId;
  private final String metricName;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Config config;
  private final boolean fetchThresholdBytesEnabled;

  // This sink is used to transfer the messages from the proxy/consumer to the BlockingEnvelopeMap.
  /* package private */final KafkaConsumerMessageSink messageSink;

  // proxy is doing the actual reading
  final private KafkaConsumerProxy proxy;

  /* package private */final Map<TopicPartition, String> topicPartitions2Offset = new HashMap<>();
  /* package private */final Map<TopicPartition, SystemStreamPartition> topicPartitions2SSP = new HashMap<>();

  /* package private */ long perPartitionFetchThreshold;
  /* package private */ long perPartitionFetchThresholdBytes;

  /**
   * Constructor
   * @param systemName system name for which we create the consumer
   * @param config config
   * @param metrics metrics
   * @param clock - system clock
   */
  public KafkaSystemConsumer(Consumer<K, V> kafkaConsumer, String systemName, Config config, String clientId,
      KafkaSystemConsumerMetrics metrics, Clock clock) {

    super(metrics.registry(), clock, metrics.getClass().getName());

    this.kafkaConsumer = kafkaConsumer;
    this.samzaConsumerMetrics = metrics;
    this.clientId = clientId;
    this.systemName = systemName;
    this.config = config;
    this.metricName = String.format("%s %s", systemName, clientId);

    this.fetchThresholdBytesEnabled = new KafkaConfig(config).isConsumerFetchThresholdBytesEnabled(systemName);

    // create a sink for passing the messages between the proxy and the consumer
    messageSink = new KafkaConsumerMessageSink();

    // Create the proxy to do the actual message reading. It is a separate thread that reads the messages from the stream
    // and puts them into the sink.
    proxy = new KafkaConsumerProxy(kafkaConsumer, systemName, clientId, messageSink, samzaConsumerMetrics, metricName);
    LOG.info("Created consumer proxy: " + proxy);

    LOG.info("Created SamzaKafkaSystemConsumer for system={}, clientId={}, metricName={}, KafkaConsumer={}", systemName,
        clientId, metricName, this.kafkaConsumer.toString());
  }

  public static <K, V> KafkaSystemConsumer getNewKafkaSystemConsumer(String systemName, Config config,
      String clientId, KafkaSystemConsumerMetrics metrics, Clock clock) {

    // extract consumer configs and create kafka consumer
    KafkaConsumer<K, V> kafkaConsumer = getKafkaConsumerImpl(systemName, clientId, config);
    LOG.info("Created kafka consumer for system {}, clientId {}: {}", systemName, clientId, kafkaConsumer);

    KafkaSystemConsumer kc = new KafkaSystemConsumer(kafkaConsumer, systemName, config, clientId, metrics, clock);
    LOG.info("Created samza system consumer {}", kc.toString());

    return kc;
  }

  /**
   * create kafka consumer
   * @param systemName system name for which we create the consumer
   * @param clientId client id to use int the kafka client
   * @param config config
   * @return kafka consumer
   */
  public static <K, V> KafkaConsumer<K, V> getKafkaConsumerImpl(String systemName, String clientId, Config config) {

    Map<String, String> injectProps = new HashMap<>();

    // extract kafka client configs
    KafkaConsumerConfig consumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, systemName, clientId, injectProps);

    LOG.info("KafkaClient properties for systemName {}: {}", systemName, consumerConfig.originals());

    return new KafkaConsumer<>(consumerConfig.originals());
  }

  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      LOG.warn("attempting to start the consumer for the second (or more) time.");
      return;
    }
    if (stopped.get()) {
      LOG.warn("attempting to start a stopped consumer");
      return;
    }
    // initialize the subscriptions for all the registered TopicPartitions
    startSubscription();
    // needs to be called after all the registrations are completed
    setFetchThresholds();

    startConsumer();
    LOG.info("consumer {} started", this);
  }

  private void startSubscription() {
    //subscribe to all the registered TopicPartitions
    LOG.info("consumer {}, subscribes to {} ", this, topicPartitions2SSP.keySet());
    try {
      synchronized (kafkaConsumer) {
        // we are using assign (and not subscribe), so we need to specify both topic and partition
        kafkaConsumer.assign(topicPartitions2SSP.keySet());
      }
    } catch (Exception e) {
      LOG.warn("startSubscription failed.", e);
      throw new SamzaException(e);
    }
  }

  /*
   Set the offsets to start from.
   Add the TopicPartitions to the proxy.
   Start the proxy thread.
   */
  void startConsumer() {
    //set the offset for each TopicPartition
    if (topicPartitions2Offset.size() <= 0) {
      LOG.warn("Consumer {} is not subscribed to any SSPs", this);
    }

    topicPartitions2Offset.forEach((tp, startingOffsetString) -> {
      long startingOffset = Long.valueOf(startingOffsetString);

      try {
        synchronized (kafkaConsumer) {
          // TODO in the future we may need to add special handling here for BEGIN/END_OFFSET
          // this will call KafkaConsumer.seekToBegin/End()
          kafkaConsumer.seek(tp, startingOffset); // this value should already be the 'upcoming' value
        }
      } catch (Exception e) {
        // all other exceptions - non recoverable
        LOG.error("Got Exception while seeking to " + startingOffsetString + " for " + tp, e);
        throw new SamzaException(e);
      }

      LOG.info("Changing consumer's starting offset for tp = " + tp + " to " + startingOffsetString);

      // add the partition to the proxy
      proxy.addTopicPartition(topicPartitions2SSP.get(tp), startingOffset);
    });

    // start the proxy thread
    if (proxy != null && !proxy.isRunning()) {
      LOG.info("Starting proxy: " + proxy);
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
      LOG.info("fetchThresholdOption is configured. fetchThreshold=" + fetchThreshold);
    }

    Option<String> fetchThresholdBytesOption = kafkaConfig.getConsumerFetchThresholdBytes(systemName);
    long fetchThresholdBytes = FETCH_THRESHOLD_BYTES;
    if (fetchThresholdBytesOption.isDefined()) {
      fetchThresholdBytes = Long.valueOf(fetchThresholdBytesOption.get());
      LOG.info("fetchThresholdBytesOption is configured. fetchThresholdBytes=" + fetchThresholdBytes);
    }

    int numTPs = topicPartitions2SSP.size();
    assert (numTPs == topicPartitions2Offset.size());

    LOG.info("fetchThresholdBytes = " + fetchThresholdBytes + "; fetchThreshold=" + fetchThreshold);
    LOG.info("number of topicPartitions " + numTPs);

    if (numTPs > 0) {
      perPartitionFetchThreshold = fetchThreshold / numTPs;
      LOG.info("perPartitionFetchThreshold=" + perPartitionFetchThreshold);
      if (fetchThresholdBytesEnabled) {
        // currently this feature cannot be enabled, because we do not have the size of the messages available.
        // messages get double buffered, hence divide by 2
        perPartitionFetchThresholdBytes = (fetchThresholdBytes / 2) / numTPs;
        LOG.info("perPartitionFetchThresholdBytes is enabled. perPartitionFetchThresholdBytes="
            + perPartitionFetchThresholdBytes);
      }
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping Samza kafkaConsumer " + this);

    if (!stopped.compareAndSet(false, true)) {
      LOG.warn("attempting to stop stopped consumer.");
      return;
    }

    // stop the proxy (with 5 minutes timeout)
    if (proxy != null) {
      LOG.info("Stopping proxy " + proxy);
      proxy.stop(TimeUnit.MINUTES.toMillis(5));
    }

    try {
      synchronized (kafkaConsumer) {
        LOG.info("Closing kafka consumer " + kafkaConsumer);
        kafkaConsumer.close();
      }
    } catch (Exception e) {
      LOG.warn("failed to stop SamzaRawKafkaConsumer + " + this, e);
    }
  }

  /*
   record the ssp and the offset. Do not submit it to the consumer yet.
   */
  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) {
    if (started.get()) {
      String msg =
          String.format("Trying to register partition after consumer has been started. sn=%s, ssp=%s", systemName,
              systemStreamPartition);
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    if (!systemStreamPartition.getSystem().equals(systemName)) {
      LOG.warn("ignoring SSP " + systemStreamPartition + ", because this consumer's system is " + systemName);
      return;
    }
    super.register(systemStreamPartition, offset);

    TopicPartition tp = toTopicPartition(systemStreamPartition);

    topicPartitions2SSP.put(tp, systemStreamPartition);

    LOG.info("Registering ssp = " + systemStreamPartition + " with offset " + offset);

    String existingOffset = topicPartitions2Offset.get(tp);
    // register the older (of the two) offset in the consumer, to guarantee we do not miss any messages.
    if (existingOffset == null || compareOffsets(existingOffset, offset) > 0) {
      topicPartitions2Offset.put(tp, offset);
    }

    samzaConsumerMetrics.registerTopicAndPartition(toTopicAndPartition(tp));
  }

  /**
   * Compare two String offsets.
   * Note. There is a method in KafkaAdmin that does that, but that would require instantiation of systemadmin for each consumer.
   * @return see {@link Long#compareTo(Long)}
   */
  public static int compareOffsets(String offset1, String offset2) {
    return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
  }

  @Override
  public String toString() {
    return systemName + "/" + clientId + "/" + super.toString();
  }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
      Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {

    // check if the proxy is running
    if (!proxy.isRunning()) {
      stop();
      if (proxy.getFailureCause() != null) {
        String message = "KafkaConsumerProxy has stopped";
        throw new SamzaException(message, proxy.getFailureCause());
      } else {
        LOG.warn("Failure cause is not populated for KafkaConsumerProxy");
        throw new SamzaException("KafkaConsumerProxy has stopped");
      }
    }

    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> res = super.poll(systemStreamPartitions, timeout);
    return res;
  }

  /**
   * convert from TopicPartition to TopicAndPartition
   */
  public static TopicAndPartition toTopicAndPartition(TopicPartition tp) {
    return new TopicAndPartition(tp.topic(), tp.partition());
  }

  /**
   * convert to TopicPartition from SystemStreamPartition
   */
  public static TopicPartition toTopicPartition(SystemStreamPartition ssp) {
    return new TopicPartition(ssp.getStream(), ssp.getPartition().getPartitionId());
  }

  /**
   * return system name for this consumer
   * @return system name
   */
  public String getSystemName() {
    return systemName;
  }

  ////////////////////////////////////
  // inner class for the message sink
  ////////////////////////////////////
  public class KafkaConsumerMessageSink {

    public void setIsAtHighWatermark(SystemStreamPartition ssp, boolean isAtHighWatermark) {
      setIsAtHead(ssp, isAtHighWatermark);
    }

    boolean needsMoreMessages(SystemStreamPartition ssp) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("needsMoreMessages from following SSP: {}. fetchLimitByBytes enabled={}; messagesSizeInQueue={};"
                + "(limit={}); messagesNumInQueue={}(limit={};", ssp, fetchThresholdBytesEnabled,
            getMessagesSizeInQueue(ssp), perPartitionFetchThresholdBytes, getNumMessagesInQueue(ssp),
            perPartitionFetchThreshold);
      }

      if (fetchThresholdBytesEnabled) {
        return getMessagesSizeInQueue(ssp) < perPartitionFetchThresholdBytes;
      } else {
        return getNumMessagesInQueue(ssp) < perPartitionFetchThreshold;
      }
    }

    void addMessage(SystemStreamPartition ssp, IncomingMessageEnvelope envelope) {
      LOG.trace("Incoming message ssp = {}: envelope = {}.", ssp, envelope);

      try {
        put(ssp, envelope);
      } catch (InterruptedException e) {
        throw new SamzaException(
            String.format("Interrupted while trying to add message with offset %s for ssp %s", envelope.getOffset(),
                ssp));
      }
    }
  }  // end of KafkaMessageSink class
  ///////////////////////////////////////////////////////////////////////////
}
