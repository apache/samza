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
package org.apache.samza.coordinator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamWriter;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


/**
 * Periodically monitors the partition count for each system stream and emits a metric
 * for each system stream indicating the delta partition count since the monitor was created.
 */
public class StreamPartitionCountMonitor {
  private static final Logger log = LoggerFactory.getLogger(StreamPartitionCountMonitor.class);

  private enum State { INIT, RUNNING, STOPPED }

  private final Set<SystemStream> streamsToMonitor;
  private final StreamMetadataCache metadataCache;
  private final MetricsRegistryMap metrics;
  private final int monitorPeriodMs;
  private final Map<SystemStream, Gauge<Integer>> gauges;
  private final Map<SystemStream, SystemStreamMetadata> initialMetadata;
  private final Config config;

  // Used to guard write access to state.
  private final Object lock = new Object();
  private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryImpl();
  private final ScheduledExecutorService schedulerService =
      Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);

  private volatile State state = State.INIT;


  /**
   * Gets the metadata for all the specified system streams from the provided metadata cache.
   * Handles scala-java conversions.
   *
   * @param streamsToMonitor  the set of system streams for which the metadata is needed.
   * @param metadataCache     the metadata cache which will be used to fetch metadata.
   * @return                  a map from each system stream to its metadata.
   */
  private static Map<SystemStream, SystemStreamMetadata> getMetadata(Set<SystemStream> streamsToMonitor,
      StreamMetadataCache metadataCache) {
    return JavaConverters
        .mapAsJavaMapConverter(
            metadataCache.getStreamMetadata(
                JavaConverters.asScalaSetConverter(streamsToMonitor).asScala().toSet(),
                true
            )
        ).asJava();
  }

  /**
   * Default constructor.
   *
   * @param streamsToMonitor  a set of SystemStreams to monitor.
   * @param metadataCache     the metadata cache which will be used to fetch metadata for partition counts.
   * @param metrics           the metrics registry to which the metrics should be added.
   * @param config            a key/value map
   */
  public StreamPartitionCountMonitor(Set<SystemStream> streamsToMonitor, StreamMetadataCache metadataCache,
      MetricsRegistryMap metrics, Config config) {
    this.streamsToMonitor = streamsToMonitor;
    this.metadataCache = metadataCache;
    this.metrics = metrics;
    this.monitorPeriodMs = new JobConfig(config).getMonitorPartitionChangeFrequency();
    this.initialMetadata = getMetadata(streamsToMonitor, metadataCache);
    this.config = config;

    // Pre-populate the gauges
    Map<SystemStream, Gauge<Integer>> mutableGauges = new HashMap<>();
    for (Map.Entry<SystemStream, SystemStreamMetadata> metadataEntry : initialMetadata.entrySet()) {
      SystemStream systemStream = metadataEntry.getKey();
      Gauge gauge = metrics.newGauge("job-coordinator",
          String.format("%s-%s-partitionCount", systemStream.getSystem(), systemStream.getStream()), 0);
      mutableGauges.put(systemStream, gauge);
    }
    gauges = Collections.unmodifiableMap(mutableGauges);
  }

  /**
   * Fetches the current partition count for each system stream from the cache, compares the current count to the
   * original count and updates the metric for that system stream with the delta.
   */
  void updatePartitionCountMetric() {
    try {
      Map<SystemStream, SystemStreamMetadata> currentMetadata = getMetadata(streamsToMonitor, metadataCache);
      boolean partitionCountIncreased = false;

      for (Map.Entry<SystemStream, SystemStreamMetadata> metadataEntry : initialMetadata.entrySet()) {
        SystemStream systemStream = metadataEntry.getKey();
        SystemStreamMetadata metadata = metadataEntry.getValue();

        int currentPartitionCount = currentMetadata.get(systemStream).getSystemStreamPartitionMetadata().keySet().size();
        int prevPartitionCount = metadata.getSystemStreamPartitionMetadata().keySet().size();

        Gauge gauge = gauges.get(systemStream);
        gauge.set(currentPartitionCount - prevPartitionCount);
        if (currentPartitionCount - prevPartitionCount > 0)
          partitionCountIncreased = true;
      }

      if (partitionCountIncreased) {
        CoordinatorStreamWriter coordinatorStreamWriter  = new CoordinatorStreamWriter(config);
        coordinatorStreamWriter.start();
        // Dong: this is a temporary solution to show how we can restart job upon partition expansion
        coordinatorStreamWriter.sendMessage(SetConfig.TYPE, "job-model-changed", "true");
        coordinatorStreamWriter.stop();
      }

    } catch (Exception e) {
      log.error("Exception while updating partition count metric.", e);
    }
  }

  /**
   * For testing. Returns the metrics.
   */
  Map<SystemStream, Gauge<Integer>> getGauges() {
    return gauges;
  }

  /**
   * Starts the monitor.
   */
  public void start() {
    synchronized (lock) {
      switch (state) {
        case INIT:
          schedulerService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
              updatePartitionCountMetric();
            }
          }, monitorPeriodMs, monitorPeriodMs, TimeUnit.MILLISECONDS);

          state = State.RUNNING;
          break;

        case RUNNING:
          // start is idempotent
          return;

        case STOPPED:
          throw new IllegalStateException("StreamPartitionCountMonitor was stopped and cannot be restarted.");
      }
    }
  }

  /**
   * Stops the monitor. Once it stops, it cannot be restarted.
   */
  public void stop() {
    synchronized (lock) {
      // We could also wait for full termination of the scheduler service, but it is overkill for
      // our use case.
      schedulerService.shutdownNow();

      state = State.STOPPED;
    }
  }

  /**
   * For testing.
   */
  boolean isRunning() {
    return state == State.RUNNING;
  }

  /**
   * Wait until this service has shutdown. Returns true if shutdown occurred within the timeout
   * and false otherwise.
   * <p>
   * This is currently exposed at the package private level for tests only.
   */
  boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return schedulerService.awaitTermination(timeout, unit);
  }

  private static class ThreadFactoryImpl implements ThreadFactory  {
    private static final String PREFIX = "Samza-" + StreamPartitionCountMonitor.class.getSimpleName() + "-";
    private static final AtomicInteger INSTANCE_NUM = new AtomicInteger();

    public Thread newThread(Runnable runnable) {
      return new Thread(runnable, PREFIX + INSTANCE_NUM.getAndIncrement());
    }
  }
}
