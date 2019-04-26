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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kafka.common.TopicAndPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
 * This class contains a separate thread that reads messages from kafka and puts them  into the BlockingEnvelopeMap
 * through KafkaSystemConsumer.KafkaConsumerMessageSink object.
 * This class is not thread safe. There will be only one instance of this class per KafkaSystemConsumer object.
 * We still need some synchronization around kafkaConsumer. See pollConsumer() method for details.
 */
public class KafkaConsumerProxy<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerProxy.class);

  private static final int SLEEP_MS_WHILE_NO_TOPIC_PARTITION = 100;

  private final Thread consumerPollThread;
  private final Consumer<K, V> kafkaConsumer;
  private final KafkaSystemConsumer.KafkaConsumerMessageSink sink;
  private final KafkaSystemConsumerMetrics kafkaConsumerMetrics;
  private final String metricName;
  private final String systemName;
  private final String clientId;
  private final Map<TopicPartition, SystemStreamPartition> topicPartitionToSSP = new HashMap<>();
  private final Map<SystemStreamPartition, MetricName> perPartitionMetrics = new HashMap<>();
  // list of all the SSPs we poll from, with their next(most recently read + 1) offsets correspondingly.
  private final Map<SystemStreamPartition, Long> nextOffsets = new ConcurrentHashMap<>();
  // lags behind the high water mark, as reported by the Kafka consumer.
  private final Map<SystemStreamPartition, Long> latestLags = new HashMap<>();

  private volatile boolean isRunning = false;
  private volatile Throwable failureCause = null;
  private final CountDownLatch consumerPollThreadStartLatch = new CountDownLatch(1);

  public KafkaConsumerProxy(Consumer<K, V> kafkaConsumer, String systemName, String clientId,
      KafkaSystemConsumer<K, V>.KafkaConsumerMessageSink messageSink, KafkaSystemConsumerMetrics samzaConsumerMetrics,
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

    LOG.info("Creating KafkaConsumerProxy with systeName={}, clientId={}, metricsName={}", systemName, clientId, metricName);
  }

  /**
   * Add new partition to the list of polled partitions.
   * Bust only be called before {@link KafkaConsumerProxy#start} is called..
   */
  public void addTopicPartition(SystemStreamPartition ssp, long nextOffset) {
    LOG.info(String.format("Adding new topicPartition %s with offset %s to queue for consumer %s", ssp, nextOffset,
        this));
    topicPartitionToSSP.put(KafkaSystemConsumer.toTopicPartition(ssp), ssp); //registered SSPs

    // this is already vetted offset so there is no need to validate it
    nextOffsets.put(ssp, nextOffset);

    kafkaConsumerMetrics.setNumTopicPartitions(metricName, nextOffsets.size());
  }

  /**
   * Stop this KafkaConsumerProxy and wait for at most {@code timeoutMs}.
   * @param timeoutMs maximum time to wait to stop this KafkaConsumerProxy
   */
  public void stop(long timeoutMs) {
    LOG.info("Shutting down KafkaConsumerProxy poll thread {} for {}", consumerPollThread.getName(), this);

    isRunning = false;
    try {
      consumerPollThread.join(timeoutMs/2);
      // join() may timeout
      // in this case we should interrupt it and wait again
      if (consumerPollThread.isAlive()) {
        consumerPollThread.interrupt();
        consumerPollThread.join(timeoutMs/2);
      }
    } catch (InterruptedException e) {
      LOG.warn("Join in KafkaConsumerProxy has failed", e);
      consumerPollThread.interrupt();
    }
  }

  public void start() {
    if (!consumerPollThread.isAlive()) {
      LOG.info("Starting KafkaConsumerProxy polling thread for " + this.toString());

      consumerPollThread.start();

      // we need to wait until the thread starts
      while (!isRunning && failureCause == null) {
        try {
          consumerPollThreadStartLatch.await(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOG.info("Ignoring InterruptedException while waiting for consumer poll thread to start.", e);
        }
      }
    } else {
      LOG.warn("Tried to start an already started KafkaConsumerProxy (%s). Ignoring.", this.toString());
    }

    if (topicPartitionToSSP.size() == 0) {
      String msg = String.format("Cannot start KafkaConsumerProxy without any registered TopicPartitions for %s", systemName);
      LOG.error(msg);
      throw new SamzaException(msg);
    }
  }

  boolean isRunning() {
    return isRunning;
  }

  Throwable getFailureCause() {
    return failureCause;
  }

  private void initializeLags() {
    // This is expensive, so only do it once at the beginning. After the first poll, we can rely on metrics for lag.
    Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitionToSSP.keySet());
    endOffsets.forEach((tp, offset) -> {
      SystemStreamPartition ssp = topicPartitionToSSP.get(tp);
      long startingOffset = nextOffsets.get(ssp);
      // End offsets are the offset of the newest message + 1
      // If the message we are about to consume is < end offset, we are starting with a lag.
      long initialLag = endOffsets.get(tp) - startingOffset;

      LOG.info("Initial lag for SSP {} is {} (end={}, startOffset={})", ssp, initialLag, endOffsets.get(tp), startingOffset);
      latestLags.put(ssp, initialLag);
      sink.setIsAtHighWatermark(ssp, initialLag == 0);
    });

    // initialize lag metrics
    refreshLagMetrics();
  }

  // creates a separate thread for getting the messages.
  private Runnable createProxyThreadRunnable() {
    return () -> {
      isRunning = true;

      try {
        consumerPollThreadStartLatch.countDown();
        LOG.info("Starting consumer poll thread {} for system {}", consumerPollThread.getName(), systemName);
        initializeLags();
        while (isRunning) {
          fetchMessages();
        }
      } catch (Throwable throwable) {
        LOG.error(String.format("Error in KafkaConsumerProxy poll thread for system: %s.", systemName), throwable);
        // KafkaSystemConsumer uses the failureCause to propagate the throwable to the container
        failureCause = throwable;
        isRunning = false;
      }

      if (!isRunning) {
        LOG.info("KafkaConsumerProxy for system {} has stopped.", systemName);
      }
    };
  }

  private void fetchMessages() {
    Set<SystemStreamPartition> sspsToFetch = new HashSet<>();
    for (SystemStreamPartition ssp : nextOffsets.keySet()) {
      if (sink.needsMoreMessages(ssp)) {
        sspsToFetch.add(ssp);
      }
    }
    LOG.debug("pollConsumer for {} SSPs: {}", sspsToFetch.size(), sspsToFetch);
    if (!sspsToFetch.isEmpty()) {
      kafkaConsumerMetrics.incClientReads(metricName);

      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> response;

      response = pollConsumer(sspsToFetch, 500L);

      // move the responses into the queue
      for (Map.Entry<SystemStreamPartition, List<IncomingMessageEnvelope>> e : response.entrySet()) {
        List<IncomingMessageEnvelope> envelopes = e.getValue();
        if (envelopes != null) {
          moveMessagesToTheirQueue(e.getKey(), envelopes);
        }
      }

      populateCurrentLags(sspsToFetch); // find current lags for for each SSP
    } else { // nothing to read

      LOG.debug("No topic/partitions need to be fetched for system {} right now. Sleeping {}ms.", systemName,
          SLEEP_MS_WHILE_NO_TOPIC_PARTITION);

      kafkaConsumerMetrics.incClientSkippedFetchRequests(metricName);

      try {
        Thread.sleep(SLEEP_MS_WHILE_NO_TOPIC_PARTITION);
      } catch (InterruptedException e) {
        LOG.warn("Sleep in fetchMessages was interrupted");
      }
    }
    refreshLagMetrics();
  }

  // the actual polling of the messages from kafka
  private Map<SystemStreamPartition, List<IncomingMessageEnvelope>> pollConsumer(
      Set<SystemStreamPartition> systemStreamPartitions, long timeoutMs) {

    // Since we need to poll only from some subset of TopicPartitions (passed as the argument),
    // we need to pause the rest.
    List<TopicPartition> topicPartitionsToPause = new ArrayList<>();
    List<TopicPartition> topicPartitionsToPoll = new ArrayList<>();

    for (Map.Entry<TopicPartition, SystemStreamPartition> e : topicPartitionToSSP.entrySet()) {
      TopicPartition tp = e.getKey();
      SystemStreamPartition ssp = e.getValue();
      if (systemStreamPartitions.contains(ssp)) {
        topicPartitionsToPoll.add(tp);  // consume
      } else {
        topicPartitionsToPause.add(tp); // ignore
      }
    }

    ConsumerRecords<K, V> records;
    try {
      // Synchronize, in case the consumer is used in some other thread (metadata or something else)
      synchronized (kafkaConsumer) {
        // Since we are not polling from ALL the subscribed topics, so we need to "change" the subscription temporarily
        kafkaConsumer.pause(topicPartitionsToPause);
        kafkaConsumer.resume(topicPartitionsToPoll);
        records = kafkaConsumer.poll(timeoutMs);
      }
    } catch (Exception e) {
      // we may get InvalidOffsetException | AuthorizationException | KafkaException exceptions,
      // but we still just rethrow, and log it up the stack.
      LOG.error("Caught a Kafka exception in pollConsumer for system " + systemName, e);
      throw e;
    }

    return processResults(records);
  }

  private Map<SystemStreamPartition, List<IncomingMessageEnvelope>> processResults(ConsumerRecords<K, V> records) {
    if (records == null) {
      throw new SamzaException("Received null 'records' after polling consumer in KafkaConsumerProxy " + this);
    }

    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> results = new HashMap<>(records.count());
    // Parse the returned records and convert them into the IncomingMessageEnvelope.
    for (ConsumerRecord<K, V> record : records) {
      int partition = record.partition();
      String topic = record.topic();
      TopicPartition tp = new TopicPartition(topic, partition);

      updateMetrics(record, tp);

      SystemStreamPartition ssp = topicPartitionToSSP.get(tp);
      List<IncomingMessageEnvelope> messages = results.computeIfAbsent(ssp, k -> new ArrayList<>());

      IncomingMessageEnvelope incomingMessageEnvelope = handleNewRecord(record, ssp);
      messages.add(incomingMessageEnvelope);
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

  /**
   * Convert a {@link ConsumerRecord} to an {@link IncomingMessageEnvelope}. This may also execute some other custom
   * logic for each new {@link IncomingMessageEnvelope}.
   *
   * This has a protected visibility so that {@link KafkaConsumerProxy} can be extended to add special handling logic
   * for custom Kafka systems.
   *
   * @param consumerRecord {@link ConsumerRecord} from Kafka that was consumed
   * @param systemStreamPartition {@link SystemStreamPartition} corresponding to the record
   * @return {@link IncomingMessageEnvelope} corresponding to the {@code consumerRecord}
   */
  protected IncomingMessageEnvelope handleNewRecord(ConsumerRecord<K, V> consumerRecord,
      SystemStreamPartition systemStreamPartition) {
    return new IncomingMessageEnvelope(systemStreamPartition, String.valueOf(consumerRecord.offset()),
        consumerRecord.key(), consumerRecord.value(), getRecordSize(consumerRecord), consumerRecord.timestamp(),
        Instant.now().toEpochMilli());
  }

  /**
   * Protected to help extensions of this class build {@link IncomingMessageEnvelope}s.
   */
  protected int getRecordSize(ConsumerRecord<K, V> r) {
    int keySize = (r.key() == null) ? 0 : r.serializedKeySize();
    return keySize + r.serializedValueSize();
  }

  private void updateMetrics(ConsumerRecord<K, V> r, TopicPartition tp) {
    TopicAndPartition tap = KafkaSystemConsumer.toTopicAndPartition(tp);
    SystemStreamPartition ssp = new SystemStreamPartition(systemName, tp.topic(), new Partition(tp.partition()));

    Long lag = latestLags.get(ssp);
    if (lag == null) {
      throw new SamzaException("Unknown/unregistered ssp in latestLags. ssp=" + ssp + "; system=" + systemName);
    }
    long currentSSPLag = lag; // lag between the current offset and the highwatermark
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

  private void moveMessagesToTheirQueue(SystemStreamPartition ssp, List<IncomingMessageEnvelope> envelopes) {
    long nextOffset = nextOffsets.get(ssp);

    for (IncomingMessageEnvelope env : envelopes) {
      sink.addMessage(ssp, env);  // move message to the BlockingEnvelopeMap's queue

      LOG.trace("IncomingMessageEnvelope. got envelope with offset:{} for ssp={}", env.getOffset(), ssp);
      nextOffset = Long.valueOf(env.getOffset()) + 1;
    }

    nextOffsets.put(ssp, nextOffset);
  }

  // The only way to figure out lag for the KafkaConsumer is to look at the metrics after each poll() call.
  // One of the metrics (records-lag) shows how far behind the HighWatermark the consumer is.
  // This method populates the lag information for each SSP into latestLags member variable.
  private void populateCurrentLags(Set<SystemStreamPartition> ssps) {

    Map<MetricName, ? extends Metric> consumerMetrics = kafkaConsumer.metrics();

    // populate the MetricNames first time
    if (perPartitionMetrics.isEmpty()) {
      for (SystemStreamPartition ssp : ssps) {
        TopicPartition tp = KafkaSystemConsumer.toTopicPartition(ssp);

        // These are required by the KafkaConsumer to get the metrics
        HashMap<String, String> tags = new HashMap<>();
        tags.put("client-id", clientId);
        tags.put("topic", tp.topic());
        tags.put("partition", Integer.toString(tp.partition()));

        perPartitionMetrics.put(ssp, new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags));
      }
    }

    for (SystemStreamPartition ssp : ssps) {
      MetricName mn = perPartitionMetrics.get(ssp);
      Metric currentLagMetric = consumerMetrics.get(mn);

      // High watermark is fixed to be the offset of last available message,
      // so the lag is now at least 0, which is the same as Samza's definition.
      // If the lag is not 0, then isAtHead is not true, and kafkaClient keeps polling.
      long currentLag = (currentLagMetric != null) ? (long) currentLagMetric.value() : -1L;
      latestLags.put(ssp, currentLag);

      // calls the setIsAtHead for the BlockingEnvelopeMap
      sink.setIsAtHighWatermark(ssp, currentLag == 0);
    }
  }

  private void refreshLagMetrics() {
    for (Map.Entry<SystemStreamPartition, Long> e : nextOffsets.entrySet()) {
      SystemStreamPartition ssp = e.getKey();
      Long offset = e.getValue();
      TopicAndPartition tp = new TopicAndPartition(ssp.getStream(), ssp.getPartition().getPartitionId());
      Long lag = latestLags.get(ssp);
      LOG.trace("Latest offset of {} is  {}; lag = {}", ssp, offset, lag);
      if (lag != null && offset != null && lag >= 0) {
        long streamEndOffset = offset + lag;
        // update the metrics
        kafkaConsumerMetrics.setHighWatermarkValue(tp, streamEndOffset);
        kafkaConsumerMetrics.setLagValue(tp, lag);
      }
    }
  }

   @Override
  public String toString() {
    return String.format("consumerProxy-%s-%s", systemName, clientId);
  }



  /**
   * Used to create an instance of {@link KafkaConsumerProxy}. This can be overridden in case an extension of
   * {@link KafkaConsumerProxy} needs to be used within kafka system components like {@link KafkaSystemConsumer}.
   */
  public static class BaseFactory<K, V> implements KafkaConsumerProxyFactory<K, V> {
    private final KafkaConsumer<K, V> kafkaConsumer;
    private final String systemName;
    private final String clientId;
    private final KafkaSystemConsumerMetrics kafkaSystemConsumerMetrics;

    public BaseFactory(KafkaConsumer<K, V> kafkaConsumer, String systemName, String clientId,
        KafkaSystemConsumerMetrics kafkaSystemConsumerMetrics) {
      this.kafkaConsumer = kafkaConsumer;
      this.systemName = systemName;
      this.clientId = clientId;
      this.kafkaSystemConsumerMetrics = kafkaSystemConsumerMetrics;
    }

    public KafkaConsumerProxy<K, V> create(KafkaSystemConsumer<K, V>.KafkaConsumerMessageSink messageSink) {
      String metricName = String.format("%s-%s", systemName, clientId);
      return new KafkaConsumerProxy<>(this.kafkaConsumer, this.systemName, this.clientId, messageSink,
          this.kafkaSystemConsumerMetrics, metricName);
    }
  }
}

