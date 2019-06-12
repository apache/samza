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

package org.apache.samza.diagnostics;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;


/**
 * Responsible for publishing data to the diagnostic stream.
 * Currently emits exception/error events obtained using a customer-appender that attaches to the root-logger.
 */
public class DiagnosticsManager {
  private static final Logger LOG = LoggerFactory.getLogger(DiagnosticsManager.class);
  private static final Duration DEFAULT_PUBLISH_PERIOD = Duration.ofSeconds(60);
  // Period size for pushing data to the diagnostic stream

  private static final String PUBLISH_THREAD_NAME = "DiagnosticsManager Thread-%d";
  private static final String SAMZACONTAINER_METRICS_GROUP_NAME = "org.apache.samza.container.SamzaContainerMetrics";
  // Using SamzaContainerMetrics as the group name for exceptions to maintain compatibility with existing diagnostics

  public static final String GROUP_NAME_FOR_DIAGNOSTICS_MANAGER = DiagnosticsManager.class.getName();
  // Using DiagnosticsManager as the group name for processor-stop-events
  public static final String STOP_EVENT_LIST_METRIC_NAME = "stopEvents";
  public static final String CONTAINER_COUNT_METRIC_NAME = "containerCount";
  public static final String CONTAINER_MB_METRIC_NAME = "containerMemoryMb";
  public static final String CONTAINER_NUM_CORES_METRIC_NAME = "containerNumCores";
  public static final String CONTAINER_NUM_STORES_METRIC_NAME = "numStores";
  public static final String CONTAINER_MODELS_METRIC_NAME = "containerModels";

  // Parameters used for populating the MetricHeader when sending diagnostic-stream messages
  private final String jobName;
  private final String jobId;
  private final String containerId;
  private final String executionEnvContainerId;
  private final String taskClassVersion;
  private final String samzaVersion;
  private final String hostname;
  private final Instant resetTime;

  // Job-related params
  private final int containerCount;
  private final int containerMemoryMb;
  private final int containerNumCores;
  private final int numStores;
  private final Map<String, ContainerModel> containerModels;
  private boolean jobParamsEmitted = false;

  private SystemProducer systemProducer; // SystemProducer for writing diagnostics data
  private final BoundedList<DiagnosticsExceptionEvent> exceptions; // A BoundedList for storing DiagnosticExceptionEvent
  private final ConcurrentLinkedQueue<ProcessorStopEvent> containerStops;
  // A BoundedList for storing DiagnosticExceptionEvent
  private final ScheduledExecutorService scheduler; // Scheduler for pushing data to the diagnostic stream
  private final Duration terminationDuration; // duration to wait when terminating the scheduler
  private final SystemStream diagnosticSystemStream;

  public DiagnosticsManager(String jobName, String jobId, Map<String, ContainerModel> containerModels, int containerCount, int containerMemoryMb,
      int containerNumCores, int numStores, String containerId, String executionEnvContainerId, String taskClassVersion,
      String samzaVersion, String hostname, SystemStream diagnosticSystemStream, SystemProducer systemProducer,
      Duration terminationDuration) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.containerModels = containerModels;
    this.containerId = containerId;
    this.executionEnvContainerId = executionEnvContainerId;
    this.taskClassVersion = taskClassVersion;
    this.samzaVersion = samzaVersion;
    this.hostname = hostname;
    resetTime = Instant.now();

    this.containerCount = containerCount;
    this.containerNumCores = containerNumCores;
    this.containerMemoryMb = containerMemoryMb;
    this.numStores = numStores;

    this.systemProducer = systemProducer;
    this.diagnosticSystemStream = diagnosticSystemStream;

    this.containerStops = new ConcurrentLinkedQueue<>();
    this.exceptions = new BoundedList<>("exceptions"); // Create a BoundedList with default size and time parameters
    this.scheduler = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(PUBLISH_THREAD_NAME).setDaemon(true).build());
    this.terminationDuration = terminationDuration;

    try {

      Util.getObj("org.apache.samza.logging.log4j.SimpleDiagnosticsAppender",
          JavaConverters.collectionAsScalaIterableConverter(
              Collections.singletonList(new Tuple2<Class<?>, Object>(DiagnosticsManager.class, this)))
              .asScala()
              .toSeq());

      LOG.info("Attached log4j diagnostics appender.");
    } catch (ClassNotFoundException | InstantiationException | InvocationTargetException e) {
      try {
        Util.getObj("org.apache.samza.logging.log4j2.SimpleDiagnosticsAppender",
            JavaConverters.collectionAsScalaIterableConverter(
                Collections.singletonList(new Tuple2<Class<?>, Object>(DiagnosticsManager.class, this)))
                .asScala()
                .toSeq());
        LOG.info("Attached log4j diagnostics appender.");
      } catch (ClassNotFoundException | InstantiationException | InvocationTargetException ex) {

        LOG.warn(
            "Failed to instantiate neither diagnostic appender for sending error information to diagnostics stream.",
            ex);
      }
    }
  }

  public void start() {
    this.scheduler.scheduleWithFixedDelay(new DiagnosticsStreamPublisher(), 0, DEFAULT_PUBLISH_PERIOD.getSeconds(),
        TimeUnit.SECONDS);
  }

  public void stop() throws InterruptedException {
    scheduler.shutdown();

    // Allow any scheduled publishes to finish, and block for termination
    scheduler.awaitTermination(terminationDuration.toMillis(), TimeUnit.MILLISECONDS);

    if (!scheduler.isTerminated()) {
      LOG.warn("Unable to terminate scheduler");
      scheduler.shutdownNow();
    }
  }

  public void addExceptionEvent(DiagnosticsExceptionEvent diagnosticsExceptionEvent) {
    this.exceptions.add(diagnosticsExceptionEvent);
  }

  public void addProcessorStopEvent(String processorId, String resourceId, String host, int exitStatus) {
    this.containerStops.add(new ProcessorStopEvent(processorId, resourceId, host, exitStatus));
    LOG.info("Added stop event for Container Id: {}, resource Id: {}, host: {}, exitStatus: {}", processorId,
        resourceId, host, exitStatus);
  }

  private class DiagnosticsStreamPublisher implements Runnable {

    @Override
    public void run() {

      Map<String, Map<String, Object>> metricsMessage = new HashMap<>();

      // Publish job-related params (if not already published)
      if (!jobParamsEmitted) {
        metricsMessage.putIfAbsent(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, new HashMap<>());
        metricsMessage.get(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER).put(CONTAINER_COUNT_METRIC_NAME, containerCount);
        metricsMessage.get(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER).put(CONTAINER_MB_METRIC_NAME, containerMemoryMb);
        metricsMessage.get(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER).put(CONTAINER_NUM_CORES_METRIC_NAME, containerNumCores);
        metricsMessage.get(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER).put(CONTAINER_NUM_STORES_METRIC_NAME, numStores);
        metricsMessage.get(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER).put(CONTAINER_MODELS_METRIC_NAME, containerModels);
      }

      // Publish processor stop-events if there are any
      List<ProcessorStopEvent> stopEventList = new ArrayList(containerStops);
      if (!stopEventList.isEmpty()) {
        metricsMessage.putIfAbsent(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER, new HashMap<>());
        metricsMessage.get(GROUP_NAME_FOR_DIAGNOSTICS_MANAGER).put(STOP_EVENT_LIST_METRIC_NAME, stopEventList);
      }

      // Publish exception events if there are any
      Collection<DiagnosticsExceptionEvent> exceptionList = exceptions.getValues();
      if (!exceptionList.isEmpty()) {
        metricsMessage.putIfAbsent(SAMZACONTAINER_METRICS_GROUP_NAME, new HashMap<>());
        metricsMessage.get(SAMZACONTAINER_METRICS_GROUP_NAME).put(exceptions.getName(), exceptionList);
      }

      if (!metricsMessage.isEmpty()) {

        // Create the metricHeader
        MetricsHeader metricsHeader =
            new MetricsHeader(jobName, jobId, "samza-container-" + containerId, executionEnvContainerId,
                DiagnosticsManager.class.getName(), taskClassVersion, samzaVersion, hostname,
                System.currentTimeMillis(), resetTime.toEpochMilli());

        MetricsSnapshot metricsSnapshot = new MetricsSnapshot(metricsHeader, new Metrics(metricsMessage));
        try {
          systemProducer.send(DiagnosticsManager.class.getName(),
              new OutgoingMessageEnvelope(diagnosticSystemStream, metricsHeader.getHost(), null,
                  new MetricsSnapshotSerdeV2().toBytes(metricsSnapshot)));

          // Remove stop events from list after successful publish
          containerStops.removeAll(stopEventList);

          // Remove exceptions from list after successful publish to diagnostics stream
          exceptions.remove(exceptionList);

          // Emit jobParams once
          jobParamsEmitted = true;
        } catch (Exception e) {
          LOG.error("Exception when flushing exceptions", e);
        }
      }
    }
  }

  public static class ProcessorStopEvent {
    public final String processorId;
    public final String resourceId;
    public final String host;
    public final int exitStatus;

    public ProcessorStopEvent(String processorId, String resourceId, String host, int exitStatus) {
      this.processorId = processorId;
      this.resourceId = resourceId;
      this.host = host;
      this.exitStatus = exitStatus;
    }
  }
}
