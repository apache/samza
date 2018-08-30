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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import kafka.common.TopicAndPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.samza.util.Clock;
import org.apache.samza.util.KafkaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;


public class NewKafkaSystemConsumer<K,V> extends BlockingEnvelopeMap implements SystemConsumer{

  private static final Logger LOG = LoggerFactory.getLogger(NewKafkaSystemConsumer.class);

  private static final long FETCH_THRESHOLD = 50000;
  private static final long FETCH_THRESHOLD_BYTES = -1L;
  private final Consumer<K,V> kafkaConsumer;
  private final String systemName;
  private final KafkaSystemConsumerMetrics samzaConsumerMetrics;
  private final String clientId;
  private final String metricName;
  /* package private */final Map<TopicPartition, SystemStreamPartition> topicPartitions2SSP = new HashMap<>();
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Config config;
  private final boolean fetchThresholdBytesEnabled;

  // This sink is used to transfer the messages from the proxy/consumer to the BlockingEnvelopeMap.
  /* package private */ KafkaConsumerMessageSink messageSink;
  // proxy is doing the actual reading
  private KafkaConsumerProxy proxy;

  /* package private */final Map<TopicPartition, String> topicPartitions2Offset = new HashMap<>();
  /* package private */long perPartitionFetchThreshold;
  /* package private */long perPartitionFetchThresholdBytes;

  // TODO - consider new class for KafkaSystemConsumerMetrics

  /**
   * @param systemName
   * @param config
   * @param metrics
   */
  public NewKafkaSystemConsumer(
      Consumer<K,V> kafkaConsumer,
      String systemName,
      Config config,
      String clientId,
      KafkaSystemConsumerMetrics metrics,
      Clock clock) {

    super(metrics.registry(),clock, metrics.getClass().getName());

    this.samzaConsumerMetrics = metrics;
    this.clientId = clientId;
    this.systemName = systemName;
    this.config = config;
    this.metricName = systemName + " " + clientId;

    this.kafkaConsumer = kafkaConsumer;

    this.fetchThresholdBytesEnabled = new KafkaConfig(config).isConsumerFetchThresholdBytesEnabled(systemName);

    LOG.info(String.format(
        "Created SamzaLiKafkaSystemConsumer for system=%s, clientId=%s, metricName=%s with liKafkaConsumer=%s",
        systemName, clientId, metricName, this.kafkaConsumer.toString()));
  }

  public static <K, V> NewKafkaSystemConsumer getNewKafkaSystemConsumer(
      String systemName,
      Config config,
      String clientId,
      KafkaSystemConsumerMetrics metrics,
      Clock clock) {

    // extract consumer configs and create kafka consumer
    KafkaConsumer<K, V> kafkaConsumer = getKafkaConsumerImpl(systemName, clientId, config);

    return new NewKafkaSystemConsumer(kafkaConsumer,
        systemName,
        config,
        clientId,
        metrics,
        clock);
  }

  /**
   * create kafka consumer
   * @param systemName
   * @param clientId
   * @param config
   * @return kafka consumer
   */
  private static <K, V> KafkaConsumer<K, V> getKafkaConsumerImpl(String systemName, String clientId, Config config) {

    Map<String, String> injectProps = new HashMap<>();

    // extract kafka consumer configs
    KafkaConsumerConfig consumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, systemName, clientId, injectProps);

    LOG.info("==============>Consumer properties in getKafkaConsumerImpl: systemName: {}, consumerProperties: {}", systemName, consumerConfig.originals());

    return new KafkaConsumer<>(consumerConfig.originals());
  }

  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      LOG.warn("attempting to start the consumer for the second (or more) time.");
      return;
    }
    if(stopped.get()) {
      LOG.warn("attempting to start a stopped consumer");
      return;
    }
    LOG.info("==============>About to start consumer");
    // initialize the subscriptions for all the registered TopicPartitions
    startSubscription();
    LOG.info("==============>subscription started");
    // needs to be called after all the registrations are completed
    setFetchThresholds();
    LOG.info("==============>thresholds ste");
    // Create the proxy to do the actual message reading. It is a separate thread that reads the messages from the stream
    // and puts them into the sink.
    createConsumerProxy();
    LOG.info("==============>proxy  started");
    startConsumer();
    LOG.info("==============>consumer started");
  }

  private void startSubscription() {
    //subscribe to all the TopicPartitions
    LOG.info("==============>startSubscription for TP: " + topicPartitions2SSP.keySet());
    try {
      synchronized (kafkaConsumer) {
        // we are using assign (and not subscribe), so we need to specify both topic and partition
        //topicPartitions2SSP.put(new TopicPartition("FAKE PARTITION", 0), new SystemStreamPartition("Some","Another", new Partition(0)));
        //topicPartitions2Offset.put(new TopicPartition("FAKE PARTITION", 0), "1234");
        kafkaConsumer.assign(topicPartitions2SSP.keySet());
      }
    } catch (Exception e) {
      LOG.warn("startSubscription failed.", e);
      throw new SamzaException(e);
    }
  }

  void createConsumerProxy() {
    // create a sink for passing the messages between the proxy and the consumer
    messageSink = new KafkaConsumerMessageSink();

    // create the thread with the consumer
    proxy = new KafkaConsumerProxy(kafkaConsumer, systemName, clientId, messageSink,
        samzaConsumerMetrics, metricName);

    LOG.info("==============>Created consumer proxy: " + proxy);
  }

  /*
   Set the offsets to start from.
   Add the TopicPartitions to the proxy.
   Start the proxy thread.
   */
  void startConsumer() {
    //set the offset for each TopicPartition
    topicPartitions2Offset.forEach((tp, startingOffsetString) -> {
      long startingOffset = Long.valueOf(startingOffsetString);

      try {
        synchronized (kafkaConsumer) {
          // TODO in the future we may need to add special handling here for BEGIN/END_OFFSET
          // this will call liKafkaConsumer.seekToBegin/End()
          kafkaConsumer.seek(tp, startingOffset); // this value should already be the 'upcoming' value
        }
      } catch (Exception e) {
        // all other exceptions - non recoverable
        LOG.error("Got Exception while seeking to " + startingOffsetString + " for " + tp, e);
        throw new SamzaException(e);
      }

      LOG.info("==============>Changing Consumer's position for tp = " + tp + " to " + startingOffsetString);

      // add the partition to the proxy
      proxy.addTopicPartition(topicPartitions2SSP.get(tp), startingOffset);
    });

    // start the proxy thread
    if (proxy != null && !proxy.isRunning()) {
      proxy.start();
    }
  }

  private void setFetchThresholds() {
    // get the thresholds, and set defaults if not defined.
    KafkaConfig kafkaConfig = new KafkaConfig(config);
    Option<String> fetchThresholdOption = kafkaConfig.getConsumerFetchThreshold(systemName);
    long fetchThreshold = FETCH_THRESHOLD;
    if(fetchThresholdOption.isDefined()) {
      fetchThreshold = Long.valueOf(fetchThresholdOption.get());
      LOG.info("fetchThresholdOption is defined. fetchThreshold=" + fetchThreshold);
    }
    Option<String> fetchThresholdBytesOption = kafkaConfig.getConsumerFetchThresholdBytes(systemName);
    long fetchThresholdBytes = FETCH_THRESHOLD_BYTES;
    if(fetchThresholdBytesOption.isDefined()) {
      fetchThresholdBytes = Long.valueOf(fetchThresholdBytesOption.get());
      LOG.info("fetchThresholdBytesOption is defined. fetchThresholdBytes=" + fetchThresholdBytes);
    }
    LOG.info("fetchThresholdBytes = " + fetchThresholdBytes + "; fetchThreshold=" + fetchThreshold);
    LOG.info("topicPartitions2Offset #=" + topicPartitions2Offset.size() + "; topicPartition2SSP #=" + topicPartitions2SSP.size());

    if (topicPartitions2SSP.size() > 0) {
      perPartitionFetchThreshold = fetchThreshold / topicPartitions2SSP.size();
      LOG.info("perPartitionFetchThreshold=" + perPartitionFetchThreshold);
      if(fetchThresholdBytesEnabled) {
        // currently this feature cannot be enabled, because we do not have the size of the messages available.
        // messages get double buffered, hence divide by 2
        perPartitionFetchThresholdBytes = (fetchThresholdBytes / 2) / topicPartitions2SSP.size();
        LOG.info("perPartitionFetchThresholdBytes is enabled. perPartitionFetchThresholdBytes=" + perPartitionFetchThresholdBytes);
      }
    }
  }

  @Override
  public void stop() {
    if (!stopped.compareAndSet(false, true)) {
      LOG.warn("attempting to stop stopped consumer.");
      return;
    }

    LOG.warn("Stopping SamzaRawLiKafkaConsumer + " + this);
    // stop the proxy (with 5 minutes timeout)
    if(proxy != null)
      proxy.stop(TimeUnit.MINUTES.toMillis(5));

    try {
      synchronized (kafkaConsumer) {
        kafkaConsumer.close();
      }
    } catch (Exception e) {
      LOG.warn("failed to stop SamzaRawLiKafkaConsumer + " + this, e);
    }
  }

  /*
   record the ssp and the offset. Do not submit it to the consumer yet.
   */
  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) {
    if (!systemStreamPartition.getSystem().equals(systemName)) {
      LOG.warn("ignoring SSP " + systemStreamPartition + ", because this consumer's system is " + systemName);
      return;
    }
    super.register(systemStreamPartition, offset);

    TopicPartition tp = toTopicPartition(systemStreamPartition);

    topicPartitions2SSP.put(tp, systemStreamPartition);

    LOG.info("==============>registering ssp = " + systemStreamPartition + " with offset " + offset);

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
   * @param off1
   * @param off2
   * @return see {@link Long#compareTo(Long)}
   */
  public static int compareOffsets(String off1, String off2) {
    return Long.valueOf(off1).compareTo(Long.valueOf(off2));
  }

  @Override
  public String toString() {
    return systemName + " " + clientId + "/" + super.toString();
  }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
      Set<SystemStreamPartition> systemStreamPartitions, long timeout)
      throws InterruptedException {

    // check if the proxy is running
    if(!proxy.isRunning()) {
      stop();
      if (proxy.getFailureCause() != null) {
        String message = "LiKafkaConsumerProxy has stopped";
        if(proxy.getFailureCause() instanceof org.apache.kafka.common.errors.TopicAuthorizationException)
          message += " due to TopicAuthorizationException Please refer to go/samzaacluserguide to correctly set up acls for your topic";
        throw new SamzaException(message, proxy.getFailureCause());
      } else {
        LOG.warn("Failure cause not populated for LiKafkaConsumerProxy");
        throw new SamzaException("LiKafkaConsumerProxy has stopped");
      }
    }

    return super.poll(systemStreamPartitions, timeout);
  }

  public static TopicAndPartition toTopicAndPartition(TopicPartition tp) {
    return new TopicAndPartition(tp.topic(), tp.partition());
  }

  public static TopicAndPartition toTopicAndPartition(SystemStreamPartition ssp) {
    return new TopicAndPartition(ssp.getStream(), ssp.getPartition().getPartitionId());
  }

  public static TopicPartition toTopicPartition(SystemStreamPartition ssp) {
    return new TopicPartition(ssp.getStream(), ssp.getPartition().getPartitionId());
  }

  public static SystemStreamPartition toSystemStreamPartition(String systemName, TopicAndPartition tp) {
    return new SystemStreamPartition(systemName, tp.topic(), new Partition(tp.partition()));
  }

  /**
   * return system name for this consumer
   * @return system name
   */
  public String getSystemName() {
    return systemName;
  }

  private static Set<SystemStream> getIntermediateStreams(Config config) {
    StreamConfig streamConfig = new StreamConfig(config);
    Collection<String> streamIds = JavaConversions.asJavaCollection(streamConfig.getStreamIds());
    return streamIds.stream()
        .filter(streamConfig::getIsIntermediateStream)
        .map(id -> streamConfig.streamIdToSystemStream(id))
        .collect(Collectors.toSet());
  }

  ////////////////////////////////////
  // inner class for the message sink
  ////////////////////////////////////
  public class KafkaConsumerMessageSink {

    public void setIsAtHighWatermark(SystemStreamPartition ssp, boolean isAtHighWatermark) {
      setIsAtHead(ssp, isAtHighWatermark);
    }

    boolean needsMoreMessages(SystemStreamPartition ssp) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("needsMoreMessages from following SSP: {}. fetchLimitByBytes enabled={}; messagesSizeInQueue={};"
                + "(limit={}); messagesNumInQueue={}(limit={};", ssp, fetchThresholdBytesEnabled, getMessagesSizeInQueue(ssp), perPartitionFetchThresholdBytes,
            getNumMessagesInQueue(ssp), perPartitionFetchThreshold);
      }

      if (fetchThresholdBytesEnabled) {
        return getMessagesSizeInQueue(ssp) < perPartitionFetchThresholdBytes; // TODO Validate
      } else {
        return getNumMessagesInQueue(ssp) < perPartitionFetchThreshold;
      }
    }

    void addMessage(SystemStreamPartition ssp, IncomingMessageEnvelope envelope) {
      LOG.info("==============>Incoming message ssp = {}: envelope = {}.", ssp, envelope);

      try {
        put(ssp, envelope);
      } catch (InterruptedException e) {
        throw new SamzaException(
            String.format("Interrupted while trying to add message with offset %s for ssp %s",
                envelope.getOffset(),
                ssp));
      }
    }
  }  // end of KafkaMessageSink class
  ///////////////////////////////////////////////////////////////////////////
}
