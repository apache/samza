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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Separate thread that reads messages from kafka and puts them into the BlockingEnvelopeMap.
 * This class is not thread safe. There will be only one instance of this class per KafkaSystemConsumer object.
 * We still need some synchronization around kafkaConsumer. See pollConsumer() method for details.
 */
public class KafkaConsumerProxy<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerProxy.class);

  private static final int SLEEP_MS_WHILE_NO_TOPIC_PARTITION = 100;

  /* package private */ final Thread consumerPollThread;
  private final Consumer<K, V> kafkaConsumer;
  private final KafkaSystemConsumer.KafkaConsumerMessageSink sink;
  private final KafkaSystemConsumerMetrics kafkaConsumerMetrics;
  private final String metricName;
  private final String systemName;
  private final String clientId;
  private final Map<TopicPartition, SystemStreamPartition> topicPartitions2SSP = new HashMap<>();
  private final Map<SystemStreamPartition, MetricName> perPartitionMetrics = new HashMap<>();
  // list of all the SSPs we poll from, with their next offsets correspondingly.
  private final Map<SystemStreamPartition, Long> nextOffsets = new ConcurrentHashMap<>();
  // lags behind the high water mark, as reported by the Kafka consumer.
  private final Map<SystemStreamPartition, Long> latestLags = new HashMap<>();

  private volatile boolean isRunning = false;
  private volatile Throwable failureCause = null;
  private final CountDownLatch consumerPollThreadStartLatch = new CountDownLatch(1);

  /* package private */KafkaConsumerProxy(Consumer<K, V> kafkaConsumer, String systemName, String clientId,
      KafkaSystemConsumer.KafkaConsumerMessageSink messageSink, KafkaSystemConsumerMetrics samzaConsumerMetrics,
      String metricName) {

    this.kafkaConsumer = kafkaConsumer;
    this.systemName = systemName;
    this.sink = messageSink;
    this.kafkaConsumerMetrics = samzaConsumerMetrics;
    this.metricName = metricName;
    this.clientId = clientId;

    this.kafkaConsumerMetrics.registerClientProxy(metricName);

    consumerPollThread = new Thread(createProxyThreadRunnable());
    consumerPollThread.setDaemon(true);
    consumerPollThread.setName(
        "Samza KafkaConsumerProxy Poll " + consumerPollThread.getName() + " - " + systemName);
  }

  public void start() {
    if (!consumerPollThread.isAlive()) {
      LOG.info("Starting KafkaConsumerProxy polling thread for system " + systemName + " " + this.toString());

      consumerPollThread.start();

      // we need to wait until the thread starts
      while (!isRunning && failureCause == null) {
        try {
          consumerPollThreadStartLatch.await(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOG.info("Got InterruptedException", e);
        }
      }
    } else {
      LOG.debug("Tried to start an already started KafkaConsumerProxy (%s). Ignoring.", this.toString());
    }
  }

  // add new partition to the list of polled partitions
  // this method is called only at the beginning, before the thread is started
  public void addTopicPartition(SystemStreamPartition ssp, long nextOffset) {
    LOG.info(String.format("Adding new topic and partition %s, offset = %s to queue for consumer %s", ssp, nextOffset,
        this));
    topicPartitions2SSP.put(KafkaSystemConsumer.toTopicPartition(ssp), ssp); //registered SSPs

    // this is already vetted offset so there is no need to validate it
    LOG.info(String.format("Got offset %s for new topic and partition %s.", nextOffset, ssp));

    nextOffsets.put(ssp, nextOffset);

    // we reuse existing metrics. They assume host and port for the broker
    // for now fake the port with the consumer name
    kafkaConsumerMetrics.setTopicPartitionValue(metricName, nextOffsets.size());
  }

  /**
   * creates a separate thread for pulling messages
   */
  private Runnable createProxyThreadRunnable() {
    Runnable runnable=  () -> {
      isRunning = true;

      try {
        consumerPollThreadStartLatch.countDown();
        LOG.info("Starting runnable " + consumerPollThread.getName());
        initializeLags();
        while (isRunning) {
          fetchMessages();
        }
      } catch (Throwable throwable) {
        LOG.error(String.format("Error in KafkaConsumerProxy poll thread for system: %s.", systemName), throwable);
        // SamzaKafkaSystemConsumer uses the failureCause to propagate the throwable to the container
        failureCause = throwable;
        isRunning = false;
      }

      if (!isRunning) {
        LOG.info("Stopping the KafkaConsumerProxy poll thread for system: {}.", systemName);
      }
    };

    return runnable;
  }

  private void initializeLags() {
    // This is expensive, so only do it once at the beginning. After the first poll, we can rely on metrics for lag.
    Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitions2SSP.keySet());
    endOffsets.forEach((tp, offset) -> {
      SystemStreamPartition ssp = topicPartitions2SSP.get(tp);
      long startingOffset = nextOffsets.get(ssp);
      // End offsets are the offset of the newest message + 1
      // If the message we are about to consume is < end offset, we are starting with a lag.
      long initialLag = endOffsets.get(tp) - startingOffset;

      LOG.info("Initial lag for SSP {} is {} (end={}, startOffset={})", ssp, initialLag, endOffsets.get(tp), startingOffset);
      latestLags.put(ssp, initialLag);
      sink.setIsAtHighWatermark(ssp, initialLag == 0);
    });

    // initialize lag metrics
    refreshLatencyMetrics();
  }

  // the actual polling of the messages from kafka
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> pollConsumer(
      Set<SystemStreamPartition> systemStreamPartitions, long timeout) {

    if (topicPartitions2SSP.size() == 0) {
      throw new SamzaException("cannot poll empty set of TopicPartitions");
    }

    // Since we need to poll only from some subset of TopicPartitions (passed as the argument),
    // we need to pause the rest.
    List<TopicPartition> topicPartitionsToPause = new ArrayList<>();
    List<TopicPartition> topicPartitionsToPoll = new ArrayList<>();

    for (Map.Entry<TopicPartition, SystemStreamPartition> e : topicPartitions2SSP.entrySet()) {
      TopicPartition tp = e.getKey();
      SystemStreamPartition ssp = e.getValue();
      if (systemStreamPartitions.contains(ssp)) {
        topicPartitionsToPoll.add(tp);  // consume
      } else {
        topicPartitionsToPause.add(tp); // ignore
      }
    }

    ConsumerRecords<K, V> records;
    // make a call on the client
    try {
      // Currently, when doing checkpoint we are making a safeOffset request through this client, thus we need to synchronize
      // them. In the future we may use this client for the actually checkpointing.
      synchronized (kafkaConsumer) {
        // Since we are not polling from ALL the subscribed topics, so we need to "change" the subscription temporarily
        kafkaConsumer.pause(topicPartitionsToPause);
        kafkaConsumer.resume(topicPartitionsToPoll);
        records = kafkaConsumer.poll(timeout);
        // resume original set of subscription - may be required for checkpointing
        kafkaConsumer.resume(topicPartitionsToPause);
      }
    } catch (InvalidOffsetException e) {
      // If the consumer has thrown this exception it means that auto reset is not set for this consumer.
      // So we just rethrow.
      LOG.error("Caught InvalidOffsetException in pollConsumer", e);
      throw e;
    } catch (KafkaException e) {
      // we may get InvalidOffsetException | AuthorizationException | KafkaException exceptions,
      // but we still just rethrow, and log it up the stack.
      LOG.error("Caught a Kafka exception in pollConsumer", e);
      throw e;
    }

    return processResults(records);
  }

  private Map<SystemStreamPartition, List<IncomingMessageEnvelope>> processResults(ConsumerRecords<K, V> records) {
    if (records == null) {
      throw new SamzaException("processResults is called with null object for records");
    }

    int capacity = (int) (records.count() / 0.75 + 1); // to avoid rehash, allocate more then 75% of expected capacity.
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> results = new HashMap<>(capacity);
    // Parse the returned records and convert them into the IncomingMessageEnvelope.
    // Note. They have been already de-serialized by the consumer.
    for (ConsumerRecord<K, V> record : records) {
      int partition = record.partition();
      String topic = record.topic();
      TopicPartition tp = new TopicPartition(topic, partition);

      updateMetrics(record, tp);

      SystemStreamPartition ssp = topicPartitions2SSP.get(tp);
      List<IncomingMessageEnvelope> listMsgs = results.get(ssp);
      if (listMsgs == null) {
        listMsgs = new ArrayList<>();
        results.put(ssp, listMsgs);
      }

      final K key = record.key();
      final Object value = record.value();
      final IncomingMessageEnvelope imEnvelope =
          new IncomingMessageEnvelope(ssp, String.valueOf(record.offset()), key, value, getRecordSize(record));
      listMsgs.add(imEnvelope);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("# records per SSP:");
      for (Map.Entry<SystemStreamPartition, List<IncomingMessageEnvelope>> e : results.entrySet()) {
        List<IncomingMessageEnvelope> list = e.getValue();
        LOG.debug(e.getKey() + " = " + ((list == null) ? 0 : list.size()));
      }
    }

    return results;
  }

  private int getRecordSize(ConsumerRecord<K, V> r) {
    int keySize = (r.key() == null) ? 0 : r.serializedKeySize();
    return keySize + r.serializedValueSize();
  }

  private void updateMetrics(ConsumerRecord<K, V> r, TopicPartition tp) {
    TopicAndPartition tap = KafkaSystemConsumer.toTopicAndPartition(tp);
    SystemStreamPartition ssp = new SystemStreamPartition(systemName, tp.topic(), new Partition(tp.partition()));
    long currentSSPLag = getLatestLag(ssp); // lag between the current offset and the highwatermark
    if (currentSSPLag < 0) {
      return;
    }
    long recordOffset = r.offset();
    long highWatermark = recordOffset + currentSSPLag; // derived value for the highwatermark

    int size = getRecordSize(r);
    kafkaConsumerMetrics.incReads(tap);
    kafkaConsumerMetrics.incBytesReads(tap, size);
    kafkaConsumerMetrics.setOffsets(tap, recordOffset);
    kafkaConsumerMetrics.incClientBytesReads(metricName, size);
    kafkaConsumerMetrics.setHighWatermarkValue(tap, highWatermark);
  }

  /*
   This method put messages into blockingEnvelopeMap.
   */
  private void moveMessagesToTheirQueue(SystemStreamPartition ssp, List<IncomingMessageEnvelope> envelopes) {
    long nextOffset = nextOffsets.get(ssp);

    for (IncomingMessageEnvelope env : envelopes) {
      sink.addMessage(ssp, env);  // move message to the BlockingEnvelopeMap's queue

      LOG.trace("IncomingMessageEnvelope. got envelope with offset:{} for ssp={}", env.getOffset(), ssp);
      nextOffset = Long.valueOf(env.getOffset()) + 1;
    }

    nextOffsets.put(ssp, nextOffset);
  }

  private void populateMetricNames(Set<SystemStreamPartition> ssps) {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("client-id", clientId);// this is required by the KafkaConsumer to get the metrics

    for (SystemStreamPartition ssp : ssps) {
      TopicPartition tp = KafkaSystemConsumer.toTopicPartition(ssp);
      perPartitionMetrics.put(ssp, new MetricName(tp + ".records-lag", "consumer-fetch-manager-metrics", "", tags));
    }
  }

  /*
    The only way to figure out lag for the KafkaConsumer is to look at the metrics after each poll() call.
    One of the metrics (records-lag) shows how far behind the HighWatermark the consumer is.
    This method populates the lag information for each SSP into latestLags member variable.
   */
  private void populateCurrentLags(Set<SystemStreamPartition> ssps) {

    Map<MetricName, ? extends Metric> consumerMetrics = kafkaConsumer.metrics();

    // populate the MetricNames first time
    if (perPartitionMetrics.isEmpty()) {
      populateMetricNames(ssps);
    }

    for (SystemStreamPartition ssp : ssps) {
      MetricName mn = perPartitionMetrics.get(ssp);
      Metric currentLagM = consumerMetrics.get(mn);

      // High watermark is fixed to be the offset of last available message,
      // so the lag is now at least 0, which is the same as Samza's definition.
      // If the lag is not 0, then isAtHead is not true, and kafkaClient keeps polling.
      long currentLag = (currentLagM != null) ? (long) currentLagM.value() : -1L;
      /*
      Metric averageLagM = consumerMetrics.get(new MetricName(tp + ".records-lag-avg", "consumer-fetch-manager-metrics", "", tags));
      double averageLag = (averageLagM != null) ? averageLagM.value() : -1.0;
      Metric maxLagM = consumerMetrics.get(new MetricName(tp + ".records-lag-max", "consumer-fetch-manager-metrics", "", tags));
      double maxLag = (maxLagM != null) ? maxLagM.value() : -1.0;
      */
      latestLags.put(ssp, currentLag);

      // calls the setIsAtHead for the BlockingEnvelopeMap
      sink.setIsAtHighWatermark(ssp, currentLag == 0);
    }
  }

  /*
    Get the latest lag for a specific SSP.
   */
  public long getLatestLag(SystemStreamPartition ssp) {
    Long lag = latestLags.get(ssp);
    if (lag == null) {
      throw new SamzaException("Unknown/unregistered ssp in latestLags request: " + ssp);
    }
    return lag;
  }

  /*
    Using the consumer to poll the messages from the stream.
   */
  private void fetchMessages() {
    Set<SystemStreamPartition> SSPsToFetch = new HashSet<>();
    for (SystemStreamPartition ssp : nextOffsets.keySet()) {
      if (sink.needsMoreMessages(ssp)) {
        SSPsToFetch.add(ssp);
      }
    }
    LOG.debug("pollConsumer {}", SSPsToFetch.size());
    if (!SSPsToFetch.isEmpty()) {
      kafkaConsumerMetrics.incClientReads(metricName);

      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> response;
      LOG.debug("pollConsumer from following SSPs: {}; total#={}", SSPsToFetch, SSPsToFetch.size());

      response = pollConsumer(SSPsToFetch, 500); // TODO should be default value from ConsumerConfig

      // move the responses into the queue
      for (Map.Entry<SystemStreamPartition, List<IncomingMessageEnvelope>> e : response.entrySet()) {
        List<IncomingMessageEnvelope> envelopes = e.getValue();
        if (envelopes != null) {
          moveMessagesToTheirQueue(e.getKey(), envelopes);
        }
      }

      populateCurrentLags(SSPsToFetch); // find current lags for for each SSP
    } else { // nothing to read

      LOG.debug("No topic/partitions need to be fetched for consumer {} right now. Sleeping {}ms.", kafkaConsumer,
          SLEEP_MS_WHILE_NO_TOPIC_PARTITION);

      kafkaConsumerMetrics.incClientSkippedFetchRequests(metricName);

      try {
        Thread.sleep(SLEEP_MS_WHILE_NO_TOPIC_PARTITION);
      } catch (InterruptedException e) {
        LOG.warn("Sleep in fetchMessages was interrupted");
      }
    }
    refreshLatencyMetrics();
  }

  private void refreshLatencyMetrics() {
    for (Map.Entry<SystemStreamPartition, Long> e : nextOffsets.entrySet()) {
      SystemStreamPartition ssp = e.getKey();
      Long offset = e.getValue();
      TopicAndPartition tp = new TopicAndPartition(ssp.getStream(), ssp.getPartition().getPartitionId());
      Long lag = latestLags.get(ssp);
      LOG.trace("Latest offset of {} is  {}; lag = {}", ssp, offset, lag);
      if (lag != null && offset != null && lag >= 0) {
        long streamEndOffset = offset.longValue() + lag.longValue();
        // update the metrics
        kafkaConsumerMetrics.setHighWatermarkValue(tp, streamEndOffset);
        kafkaConsumerMetrics.setLagValue(tp, lag.longValue());
      }
    }
  }

  boolean isRunning() {
    return isRunning;
  }

  Throwable getFailureCause() {
    return failureCause;
  }

  public void stop(long timeout) {
    LOG.info("Shutting down KafkaConsumerProxy poll thread:" + consumerPollThread.getName());

    isRunning = false;
    try {
      consumerPollThread.join(timeout);
    } catch (InterruptedException e) {
      LOG.warn("Join in KafkaConsumerProxy has failed", e);
      consumerPollThread.interrupt();
    }
  }
}

