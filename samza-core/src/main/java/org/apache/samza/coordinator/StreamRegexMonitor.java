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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


/**
 * Periodically monitors input regexes
 */
public class StreamRegexMonitor {
  private static final Logger log = LoggerFactory.getLogger(StreamRegexMonitor.class);

  private enum State { INIT, RUNNING, STOPPED }

  private final Set<SystemStream> streamsToMonitor;
  private final Map<String, Pattern> systemRegexesToMonitor;
  private final StreamMetadataCache metadataCache;
  private final int inputRegexMonitorPeriodMs;

  // Map of gauges (one per system), emitted when new input stream for that system is detected
  private final Map<String, Gauge<Integer>> gauges;

  private final Callback callbackMethod;

  // Used to guard write access to state.
  private final Object lock = new Object();

  // Factory of daemon-threads to create the single threaded executor pool
  private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("Samza-" + StreamRegexMonitor.class.getSimpleName())
      .build();
  private final ScheduledExecutorService schedulerService = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);

  private volatile State state = State.INIT;

  /**
   * A callback that is invoked when the {@link StreamRegexMonitor} detects a new input stream matching given regex.
   */
  public interface Callback {
    /**
     * Method to be called when new input streams are detected.
     * @param initialInputSet The initial set of input streams
     * @param newInputStreams The set of new input streams discovered
     * @param regexesMonitored The set of regexes being monitored
     */
    void onInputStreamsChanged(Set<SystemStream> initialInputSet, Set<SystemStream> newInputStreams,
        Map<String, Pattern> regexesMonitored);
  }

  /**
   * Gets the metadata for all the specified system streams from the provided metadata cache.
   * Handles scala-java conversions.
   *
   * @param streamsToMonitor  the set of system streams for which the metadata is needed.
   * @param metadataCache     the metadata cache which will be used to fetch metadata.
   * @return a map from each system stream to its metadata.
   */
  private static Map<SystemStream, SystemStreamMetadata> getMetadata(Set<SystemStream> streamsToMonitor,
      StreamMetadataCache metadataCache) {
    return JavaConverters.mapAsJavaMapConverter(
        metadataCache.getStreamMetadata(JavaConverters.asScalaSetConverter(streamsToMonitor).asScala().toSet(), true))
        .asJava();
  }

  /**
   * Default constructor.
   *
   *  @param streamsToMonitor  a set of SystemStreams to monitor
   * @param systemRegexesToMonitor  map of regexes for each input system
   * @param metadataCache     the metadata cache which will be used to fetch metadata for partition counts.
   * @param metrics           the metrics registry to which the metrics should be added.
   * @param inputRegexMonitorPeriodMs the period at which the monitor will check each input-regex
   * @param monitorCallback   the callback method to be invoked when new input stream matching regex is detected
   */
  public StreamRegexMonitor(Set<SystemStream> streamsToMonitor, Map<String, Pattern> systemRegexesToMonitor,
      StreamMetadataCache metadataCache, MetricsRegistry metrics, int inputRegexMonitorPeriodMs,
      Callback monitorCallback) {
    this.streamsToMonitor = streamsToMonitor;
    this.systemRegexesToMonitor = systemRegexesToMonitor;
    this.metadataCache = metadataCache;
    this.callbackMethod = monitorCallback;
    this.inputRegexMonitorPeriodMs = inputRegexMonitorPeriodMs;

    // Pre-populate the gauges
    Map<String, Gauge<Integer>> mutableGauges = new HashMap<>();
    for (String systemToMonitor : systemRegexesToMonitor.keySet()) {
      Gauge gauge = metrics.newGauge("job-coordinator", String.format("%s-new-input-streams", systemToMonitor), 0);
      mutableGauges.put(systemToMonitor, gauge);
    }
    gauges = Collections.unmodifiableMap(mutableGauges);

    log.info("Created {} with inputRegexMonitorPeriodMs: {} and systemRegexesToMonitor: {}", this.getClass().getName(),
        this.inputRegexMonitorPeriodMs, this.systemRegexesToMonitor);
  }

  /**
   * Starts the monitor.
   */
  public void start() {
    synchronized (lock) {
      switch (state) {
        case INIT:
          if (inputRegexMonitorPeriodMs > 0) {
            schedulerService.scheduleAtFixedRate(new Runnable() {
              @Override
              public void run() {
                monitorInputRegexes();
              }
            }, 0, inputRegexMonitorPeriodMs, TimeUnit.MILLISECONDS);
          }
          state = State.RUNNING;
          break;

        case RUNNING:
          // start is idempotent
          return;

        case STOPPED:
          throw new IllegalStateException("StreamRegexMonitor was stopped and cannot be restarted.");
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

  private void monitorInputRegexes() {
    log.debug("Running monitorInputRegexes");

    try {
      // obtain the list of SysStreams that match given patterns for all systems
      Set<SystemStream> inputStreamsMatchingPattern = new HashSet<>();

      // For each input system, for which we have a regex to monitor
      for (String systemName : this.systemRegexesToMonitor.keySet()) {

        try {
          // obtain the list of SysStreams that match the regex for this system
          // using the systemAdmin in the metadataCache
          inputStreamsMatchingPattern.addAll(
              JavaConverters.setAsJavaSetConverter(this.metadataCache.getAllSystemStreams(systemName))
                  .asJava()
                  .stream()
                  .filter(x -> x.getStream().matches(this.systemRegexesToMonitor.get(systemName).pattern()))
                  .collect(Collectors.toSet()));
        } catch (UnsupportedOperationException e) {
          log.error("UnsupportedOperationException while monitoring input regexes for system {}", systemName, e);
        }
      }

      // if there is a stream that is in the input-Set but not in the streamsToMonitor
      // since streamsToMonitor = task.inputs
      if (!streamsToMonitor.containsAll(inputStreamsMatchingPattern)) {
        log.info("New input system-streams discovered. InputStreamsMatchingPattern: {} but streamsToMonitor: {} ",
            inputStreamsMatchingPattern, streamsToMonitor);

        // invoke notify callback with new input streams
        this.callbackMethod.onInputStreamsChanged(streamsToMonitor,
            Sets.difference(inputStreamsMatchingPattern, streamsToMonitor), systemRegexesToMonitor);
      } else {
        log.info("No new input system-Streams discovered streamsToMonitor {} inputStreamsMatchingPattern {}",
            streamsToMonitor, inputStreamsMatchingPattern);
      }
    } catch (Exception e) {
      log.error("Exception while monitoring input regexes.", e);
    }
  }

  @VisibleForTesting
  boolean isRunning() {
    return state == State.RUNNING;
  }

  /**
   * Wait until this service has shutdown. Returns true if shutdown occurred within the timeout
   * and false otherwise.
   * <p>
   * This is currently exposed at the package private level for tests only.
   */
  @VisibleForTesting
  boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return schedulerService.awaitTermination(timeout, unit);
  }
}
