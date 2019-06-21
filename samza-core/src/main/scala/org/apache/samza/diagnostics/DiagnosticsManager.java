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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.samza.job.model.ContainerModel;
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
  private final int containerMemoryMb;
  private final int containerNumCores;
  private final int numStoresWithChangelog;
  private final Map<String, ContainerModel> containerModels;
  private boolean jobParamsEmitted = false;

  private SystemProducer systemProducer; // SystemProducer for writing diagnostics data
  private final BoundedList<DiagnosticsExceptionEvent> exceptions; // A BoundedList for storing DiagnosticExceptionEvent
  private final ConcurrentLinkedQueue<ProcessorStopEvent> processorStopEvents;
  // A BoundedList for storing DiagnosticExceptionEvent
  private final ScheduledExecutorService scheduler; // Scheduler for pushing data to the diagnostic stream
  private final Duration terminationDuration; // duration to wait when terminating the scheduler
  private final SystemStream diagnosticSystemStream;

  public DiagnosticsManager(String jobName, String jobId, Map<String, ContainerModel> containerModels,
      int containerMemoryMb, int containerNumCores, int numStoresWithChangelog, String containerId,
      String executionEnvContainerId, String taskClassVersion, String samzaVersion, String hostname,
      SystemStream diagnosticSystemStream, SystemProducer systemProducer, Duration terminationDuration) {

    this(jobName, jobId, containerModels, containerMemoryMb, containerNumCores, numStoresWithChangelog, containerId,
        executionEnvContainerId, taskClassVersion, samzaVersion, hostname, diagnosticSystemStream, systemProducer,
        terminationDuration, Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat(PUBLISH_THREAD_NAME).setDaemon(true).build()));
  }

  public DiagnosticsManager(String jobName, String jobId, Map<String, ContainerModel> containerModels,
      int containerMemoryMb, int containerNumCores, int numStoresWithChangelog, String containerId,
      String executionEnvContainerId, String taskClassVersion, String samzaVersion, String hostname,
      SystemStream diagnosticSystemStream, SystemProducer systemProducer, Duration terminationDuration,
      ScheduledExecutorService executorService) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.containerModels = containerModels;
    this.containerMemoryMb = containerMemoryMb;
    this.containerNumCores = containerNumCores;
    this.numStoresWithChangelog = numStoresWithChangelog;
    this.containerId = containerId;
    this.executionEnvContainerId = executionEnvContainerId;
    this.taskClassVersion = taskClassVersion;
    this.samzaVersion = samzaVersion;
    this.hostname = hostname;
    this.diagnosticSystemStream = diagnosticSystemStream;
    this.systemProducer = systemProducer;
    this.terminationDuration = terminationDuration;

    this.processorStopEvents = new ConcurrentLinkedQueue<>();
    this.exceptions = new BoundedList<>("exceptions"); // Create a BoundedList with default size and time parameters
    this.scheduler = executorService;

    resetTime = Instant.now();

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
    this.processorStopEvents.add(new ProcessorStopEvent(processorId, resourceId, host, exitStatus));
    LOG.info("Added stop event for Container Id: {}, resource Id: {}, host: {}, exitStatus: {}", processorId,
        resourceId, host, exitStatus);
  }

  private class DiagnosticsStreamPublisher implements Runnable {

    @Override
    public void run() {
      try {
        DiagnosticsStreamMessage diagnosticsStreamMessage =
            new DiagnosticsStreamMessage(jobName, jobId, "samza-container-" + containerId, executionEnvContainerId,
                taskClassVersion, samzaVersion, hostname, System.currentTimeMillis(), resetTime.toEpochMilli());

        // Add job-related params to the message (if not already published)
        if (!jobParamsEmitted) {
          diagnosticsStreamMessage.addContainerMb(containerMemoryMb);
          diagnosticsStreamMessage.addContainerNumCores(containerNumCores);
          diagnosticsStreamMessage.addNumStoresWithChangelog(numStoresWithChangelog);
          diagnosticsStreamMessage.addContainerModels(containerModels);
        }

        // Add stop event list to the message
        diagnosticsStreamMessage.addProcessorStopEvents(new ArrayList(processorStopEvents));

        // Add exception events to the message
        diagnosticsStreamMessage.addDiagnosticsExceptionEvents(exceptions.getValues());

        if (!diagnosticsStreamMessage.isEmpty()) {

          systemProducer.send(DiagnosticsManager.class.getName(),
              new OutgoingMessageEnvelope(diagnosticSystemStream, hostname, null,
                  new MetricsSnapshotSerdeV2().toBytes(diagnosticsStreamMessage.convertToMetricsSnapshot())));

          // Remove stop events from list after successful publish
          processorStopEvents.removeAll(diagnosticsStreamMessage.getProcessorStopEvents());

          // Remove exceptions from list after successful publish to diagnostics stream
          exceptions.remove(diagnosticsStreamMessage.getExceptionEvents());

          // Emit jobParams once
          jobParamsEmitted = true;
        }
      } catch (Exception e) {
        LOG.error("Exception when flushing diagnosticsStreamMessage", e);
      }
    }
  }

  public static class ProcessorStopEvent {
    public final String processorId;
    public final String resourceId;
    public final String host;
    public final int exitStatus;

    private ProcessorStopEvent() {
      this("", "", "", -1);
    }

    public ProcessorStopEvent(String processorId, String resourceId, String host, int exitStatus) {
      this.processorId = processorId;
      this.resourceId = resourceId;
      this.host = host;
      this.exitStatus = exitStatus;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ProcessorStopEvent that = (ProcessorStopEvent) o;
      return exitStatus == that.exitStatus && Objects.equals(processorId, that.processorId) && Objects.equals(
          resourceId, that.resourceId) && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
      return Objects.hash(processorId, resourceId, host, exitStatus);
    }
  }
}
