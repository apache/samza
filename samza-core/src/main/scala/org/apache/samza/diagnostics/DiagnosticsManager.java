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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.samza.config.Config;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Clock;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private final String samzaEpochId;
  private final String taskClassVersion;
  private final String samzaVersion;
  private final String hostname;
  private final Instant resetTime;

  // Job-related params
  private final int containerMemoryMb;
  private final int containerNumCores;
  private final int numPersistentStores;
  private final long maxHeapSizeBytes;
  private final int containerThreadPoolSize;
  private final Map<String, ContainerModel> containerModels;
  private final boolean autosizingEnabled;
  private final Config config;
  private final Clock clock;
  private boolean jobParamsEmitted = false;

  private final SystemProducer systemProducer; // SystemProducer for writing diagnostics data
  private final BoundedList<DiagnosticsExceptionEvent> exceptions; // A BoundedList for storing DiagnosticExceptionEvent
  private final ConcurrentLinkedQueue<ProcessorStopEvent> processorStopEvents;
  // A BoundedList for storing DiagnosticExceptionEvent
  private final ScheduledExecutorService scheduler; // Scheduler for pushing data to the diagnostic stream
  private final Duration terminationDuration; // duration to wait when terminating the scheduler
  private final SystemStream diagnosticSystemStream;

  public DiagnosticsManager(String jobName,
      String jobId,
      Map<String, ContainerModel> containerModels,
      int containerMemoryMb,
      int containerNumCores,
      int numPersistentStores,
      long maxHeapSizeBytes,
      int containerThreadPoolSize,
      String containerId,
      String executionEnvContainerId,
      String samzaEpochId,
      String taskClassVersion,
      String samzaVersion,
      String hostname,
      SystemStream diagnosticSystemStream,
      SystemProducer systemProducer,
      Duration terminationDuration,
      boolean autosizingEnabled,
      Config config) {

    this(jobName, jobId, containerModels, containerMemoryMb, containerNumCores, numPersistentStores, maxHeapSizeBytes,
        containerThreadPoolSize, containerId, executionEnvContainerId, samzaEpochId, taskClassVersion,
        samzaVersion, hostname, diagnosticSystemStream, systemProducer, terminationDuration,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat(PUBLISH_THREAD_NAME).setDaemon(true).build()), autosizingEnabled,
        config, SystemClock.instance());
  }

  @VisibleForTesting
  DiagnosticsManager(String jobName,
      String jobId,
      Map<String, ContainerModel> containerModels,
      int containerMemoryMb,
      int containerNumCores,
      int numPersistentStores,
      long maxHeapSizeBytes,
      int containerThreadPoolSize,
      String containerId,
      String executionEnvContainerId,
      String samzaEpochId,
      String taskClassVersion,
      String samzaVersion,
      String hostname,
      SystemStream diagnosticSystemStream,
      SystemProducer systemProducer,
      Duration terminationDuration,
      ScheduledExecutorService executorService,
      boolean autosizingEnabled,
      Config config,
      Clock clock) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.containerModels = containerModels;
    this.containerMemoryMb = containerMemoryMb;
    this.containerNumCores = containerNumCores;
    this.numPersistentStores = numPersistentStores;
    this.maxHeapSizeBytes = maxHeapSizeBytes;
    this.containerThreadPoolSize = containerThreadPoolSize;
    this.containerId = containerId;
    this.executionEnvContainerId = executionEnvContainerId;
    this.samzaEpochId = samzaEpochId;
    this.taskClassVersion = taskClassVersion;
    this.samzaVersion = samzaVersion;
    this.hostname = hostname;
    this.diagnosticSystemStream = diagnosticSystemStream;
    this.systemProducer = systemProducer;
    this.terminationDuration = terminationDuration;

    this.processorStopEvents = new ConcurrentLinkedQueue<>();
    this.exceptions = new BoundedList<>("exceptions"); // Create a BoundedList with default size and time parameters
    this.scheduler = executorService;
    this.autosizingEnabled = autosizingEnabled;
    this.config = config;
    this.clock = clock;

    this.resetTime = Instant.ofEpochMilli(this.clock.currentTimeMillis());
    this.systemProducer.register(getClass().getSimpleName());

    try {
      ReflectionUtil.getObjWithArgs("org.apache.samza.logging.log4j.SimpleDiagnosticsAppender",
          Object.class, ReflectionUtil.constructorArgument(this, DiagnosticsManager.class));
      LOG.info("Attached log4j diagnostics appender.");
    } catch (Exception e) {
      try {
        ReflectionUtil.getObjWithArgs("org.apache.samza.logging.log4j2.SimpleDiagnosticsAppender",
            Object.class, ReflectionUtil.constructorArgument(this, DiagnosticsManager.class));
        LOG.info("Attached log4j2 diagnostics appender.");
      } catch (Exception ex) {
        LOG.warn(
            "Failed to instantiate neither diagnostic appender for sending error information to diagnostics stream.",
            ex);
      }
    }
  }

  public void start() {
    this.systemProducer.start();
    this.scheduler.scheduleWithFixedDelay(new DiagnosticsStreamPublisher(), 0, DEFAULT_PUBLISH_PERIOD.getSeconds(),
        TimeUnit.SECONDS);
  }

  public void stop() throws InterruptedException {
    try {
      scheduler.shutdown();
      // Allow any scheduled publishes to finish, and block for termination
      scheduler.awaitTermination(terminationDuration.toMillis(), TimeUnit.MILLISECONDS);
    } finally {
      if (!scheduler.isTerminated()) {
        LOG.warn("Unable to terminate scheduler");
        scheduler.shutdownNow();
      }
      this.systemProducer.stop();
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
                Optional.of(samzaEpochId), taskClassVersion, samzaVersion, hostname,
                clock.currentTimeMillis(), resetTime.toEpochMilli());

        // Add job-related params to the message (if not already published)
        if (!jobParamsEmitted) {
          diagnosticsStreamMessage.addContainerMb(containerMemoryMb);
          diagnosticsStreamMessage.addContainerNumCores(containerNumCores);
          diagnosticsStreamMessage.addNumPersistentStores(numPersistentStores);
          diagnosticsStreamMessage.addContainerModels(containerModels);
          diagnosticsStreamMessage.addMaxHeapSize(maxHeapSizeBytes);
          diagnosticsStreamMessage.addContainerThreadPoolSize(containerThreadPoolSize);
          diagnosticsStreamMessage.addAutosizingEnabled(autosizingEnabled);
          diagnosticsStreamMessage.addConfig(config);
        }

        // Add stop event list to the message
        diagnosticsStreamMessage.addProcessorStopEvents(new ArrayList(processorStopEvents));

        // Add exception events to the message
        diagnosticsStreamMessage.addDiagnosticsExceptionEvents(exceptions.getValues());

        if (!diagnosticsStreamMessage.isEmpty()) {

          systemProducer.send(DiagnosticsManager.class.getName(),
              new OutgoingMessageEnvelope(diagnosticSystemStream, hostname, null,
                  new MetricsSnapshotSerdeV2().toBytes(diagnosticsStreamMessage.convertToMetricsSnapshot())));
          systemProducer.flush(DiagnosticsManager.class.getName());

          // Remove stop events from list after successful publish
          if (diagnosticsStreamMessage.getProcessorStopEvents() != null) {
            processorStopEvents.removeAll(diagnosticsStreamMessage.getProcessorStopEvents());
          }

          // Remove exceptions from list after successful publish to diagnostics stream
          if (diagnosticsStreamMessage.getExceptionEvents() != null) {
            exceptions.remove(diagnosticsStreamMessage.getExceptionEvents());
          }

          // Emit jobParams once
          jobParamsEmitted = true;
        }
      } catch (Exception e) {
        LOG.error("Exception when flushing diagnosticsStreamMessage", e);
      }
    }
  }

}
