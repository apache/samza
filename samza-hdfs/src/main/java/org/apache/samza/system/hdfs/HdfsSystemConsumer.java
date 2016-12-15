/*
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
*/

package org.apache.samza.system.hdfs;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.Validate;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.hdfs.reader.HdfsReaderFactory;
import org.apache.samza.system.hdfs.reader.MultiFileHdfsReader;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;


/**
 * The system consumer for HDFS, extending the {@link org.apache.samza.util.BlockingEnvelopeMap}.
 * Events will be parsed from HDFS and placed into a blocking queue in {@link org.apache.samza.util.BlockingEnvelopeMap}.
 * There will be one {@link org.apache.samza.system.hdfs.reader.MultiFileHdfsReader} for each {@link org.apache.samza.system.SystemStreamPartition},
 * each {@link org.apache.samza.system.hdfs.reader.MultiFileHdfsReader} is running within its own thread.
 *
 *    ┌───────────────────────────────────────┐            ┌─────────────────────┐
 *    │                                       │            │                     │
 *    │    MultiFileHdfsReader_1 - Thread1    │───────────▶│ SSP1-BlockingQueue  ├──────┐
 *    │                                       │            │                     │      │
 *    └───────────────────────────────────────┘            └─────────────────────┘      │
 *                                                                                      │
 *    ┌───────────────────────────────────────┐            ┌─────────────────────┐      │
 *    │                                       │            │                     │      │
 *    │    MultiFileHdfsReader_2 - Thread2    │───────────▶│ SSP2-BlockingQueue  ├──────┤        ┌──────────────────────────┐
 *    │                                       │            │                     │      ├───────▶│                          │
 *    └───────────────────────────────────────┘            └─────────────────────┘      └───────▶│  SystemConsumer.poll()   │
 *                                                                                      ┌───────▶│                          │
 *                                                                                      │        └──────────────────────────┘
 *                       ...                                         ...                │
 *                                                                                      │
 *                                                                                      │
 *    ┌───────────────────────────────────────┐            ┌─────────────────────┐      │
 *    │                                       │            │                     │      │
 *    │    MultiFileHdfsReader_N - ThreadN    │───────────▶│ SSPN-BlockingQueue  ├──────┘
 *    │                                       │            │                     │
 *    └───────────────────────────────────────┘            └─────────────────────┘
 * Since each thread has only one reader and has its own blocking queue, there are essentially no communication
 * among reader threads.
 * Thread safety between reader threads and Samza main thread is guaranteed by the blocking queues stand in the middle.
 */
public class HdfsSystemConsumer extends BlockingEnvelopeMap {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsSystemConsumer.class);

  private static final String METRICS_GROUP_NAME = HdfsSystemConsumer.class.getName();

  private final HdfsReaderFactory.ReaderType readerType;
  private final String stagingDirectory; // directory that contains the partition description
  private final int bufferCapacity;
  private final int numMaxRetires;
  private ExecutorService executorService;

  /**
   * The cached map collection from stream partition to partition descriptor. The partition descriptor
   * is the actual file path (or the set of file paths if the partition contains multiple files)
   * of the stream partition.
   * For example,
   * (stream1) -> (P0) -> "hdfs://user/samzauser/1/datafile01.avro"
   * (stream1) -> (P1) -> "hdfs://user/samzauser/1/datafile02.avro"
   * (stream2) -> (P0) -> "hdfs://user/samzauser/2/datafile01.avro"
   * ...
   */
  private LoadingCache<String, Map<Partition, List<String>>> cachedPartitionDescriptorMap;
  private Map<SystemStreamPartition, MultiFileHdfsReader> readers;
  private Map<SystemStreamPartition, Future> readerRunnableStatus;

  /**
   * Whether the {@link org.apache.samza.system.hdfs.HdfsSystemConsumer} is notified
   * to be shutdown. {@link org.apache.samza.system.hdfs.HdfsSystemConsumer.ReaderRunnable} on
   * each thread will be checking this variable to determine whether it should stop.
   */
  private volatile boolean isShutdown;

  private final HdfsSystemConsumerMetrics consumerMetrics;
  private final HdfsConfig hdfsConfig;

  public HdfsSystemConsumer(String systemName, Config config, HdfsSystemConsumerMetrics consumerMetrics) {
    super(consumerMetrics.getMetricsRegistry());
    hdfsConfig = new HdfsConfig(config);
    readerType = HdfsReaderFactory.getType(hdfsConfig.getFileReaderType(systemName));
    stagingDirectory = hdfsConfig.getStagingDirectory();
    bufferCapacity = hdfsConfig.getConsumerBufferCapacity(systemName);
    numMaxRetires = hdfsConfig.getConsumerNumMaxRetries(systemName);
    readers = new ConcurrentHashMap<>();
    readerRunnableStatus = new ConcurrentHashMap<>();
    isShutdown = false;
    this.consumerMetrics = consumerMetrics;
    cachedPartitionDescriptorMap = CacheBuilder.newBuilder().build(new CacheLoader<String, Map<Partition, List<String>>>() {
        @Override
        public Map<Partition, List<String>> load(String streamName)
          throws Exception {
          Validate.notEmpty(streamName);
          return HdfsSystemAdmin.obtainPartitionDescriptorMap(stagingDirectory, streamName);
        }
      });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start() {
    LOG.info(String.format("HdfsSystemConsumer started with %d readers", readers.size()));
    executorService = Executors.newCachedThreadPool();
    readers.entrySet().forEach(
      entry -> readerRunnableStatus.put(entry.getKey(), executorService.submit(new ReaderRunnable(entry.getValue()))));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() {
    LOG.info("Received request to stop HdfsSystemConsumer.");
    isShutdown = true;
    executorService.shutdown();
    LOG.info("HdfsSystemConsumer stopped.");
  }

  private List<String> getPartitionDescriptor(SystemStreamPartition systemStreamPartition) {
    String streamName = systemStreamPartition.getStream();
    Partition partition = systemStreamPartition.getPartition();
    try {
      return cachedPartitionDescriptorMap.get(streamName).get(partition);
    } catch (ExecutionException e) {
      throw new SamzaException("Failed to obtain descriptor for " + systemStreamPartition, e);
    }
  }

  @Override
  protected BlockingQueue<IncomingMessageEnvelope> newBlockingQueue() {
    return new LinkedBlockingQueue<>(bufferCapacity);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) {
    LOG.info("HdfsSystemConsumer register with partition: " + systemStreamPartition + " and offset " + offset);
    super.register(systemStreamPartition, offset);
    MultiFileHdfsReader reader =
      new MultiFileHdfsReader(readerType, systemStreamPartition, getPartitionDescriptor(systemStreamPartition), offset,
        numMaxRetires);
    readers.put(systemStreamPartition, reader);
    consumerMetrics.registerSystemStreamPartition(systemStreamPartition);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
    Set<SystemStreamPartition> systemStreamPartitions, long timeout)
    throws InterruptedException {
    systemStreamPartitions.forEach(systemStreamPartition -> {
      Future status = readerRunnableStatus.get(systemStreamPartition);
      if (status.isDone()) {
        try {
          status.get();
        } catch (ExecutionException | InterruptedException e) {
          MultiFileHdfsReader reader = readers.get(systemStreamPartition);
          LOG.warn(
            String.format("Detect failure in ReaderRunnable for ssp: %s. Try to reconnect now.", systemStreamPartition),
            e);
          reader.reconnect();
          readerRunnableStatus.put(systemStreamPartition, executorService.submit(new ReaderRunnable(reader)));
        }
      }
    });
    return super.poll(systemStreamPartitions, timeout);
  }

  private void offerMessage(SystemStreamPartition systemStreamPartition, IncomingMessageEnvelope envelope) {
    try {
      super.put(systemStreamPartition, envelope);
    } catch (InterruptedException e) {
      throw new SamzaException("ReaderRunnable interrupted for ssp: " + systemStreamPartition);
    }
  }

  private void doPoll(MultiFileHdfsReader reader) {
    SystemStreamPartition systemStreamPartition = reader.getSystemStreamPartition();
    while (reader.hasNext() && !isShutdown) {
      IncomingMessageEnvelope messageEnvelope = reader.readNext();
      offerMessage(systemStreamPartition, messageEnvelope);
      consumerMetrics.incNumEvents(systemStreamPartition);
      consumerMetrics.incTotalNumEvents();
    }
    offerMessage(systemStreamPartition, IncomingMessageEnvelope.buildEndOfStreamEnvelope(systemStreamPartition));
    reader.close();
  }

  public static class HdfsSystemConsumerMetrics {

    private final MetricsRegistry metricsRegistry;
    private final Map<SystemStreamPartition, Counter> numEventsCounterMap;
    private final Counter numTotalEventsCounter;

    public HdfsSystemConsumerMetrics(MetricsRegistry metricsRegistry) {
      this.metricsRegistry = metricsRegistry;
      this.numEventsCounterMap = new ConcurrentHashMap<>();
      this.numTotalEventsCounter = metricsRegistry.newCounter(METRICS_GROUP_NAME, "num-total-events");
    }

    public void registerSystemStreamPartition(SystemStreamPartition systemStreamPartition) {
      numEventsCounterMap.putIfAbsent(systemStreamPartition,
        metricsRegistry.newCounter(METRICS_GROUP_NAME, "num-events-" + systemStreamPartition));
    }

    public void incNumEvents(SystemStreamPartition systemStreamPartition) {
      if (!numEventsCounterMap.containsKey(systemStreamPartition)) {
        registerSystemStreamPartition(systemStreamPartition);
      }
      numEventsCounterMap.get(systemStreamPartition).inc();
    }

    public void incTotalNumEvents() {
      numTotalEventsCounter.inc();
    }

    public MetricsRegistry getMetricsRegistry() {
      return metricsRegistry;
    }
  }

  private class ReaderRunnable implements Runnable {
    public MultiFileHdfsReader reader;

    public ReaderRunnable(MultiFileHdfsReader reader) {
      this.reader = reader;
    }

    @Override
    public void run() {
      doPoll(reader);
    }
  }
}
