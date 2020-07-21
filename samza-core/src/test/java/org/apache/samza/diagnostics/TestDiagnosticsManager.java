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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class TestDiagnosticsManager {
  private DiagnosticsManager diagnosticsManager;
  private MockSystemProducer mockSystemProducer;
  private ScheduledExecutorService mockExecutorService;
  private SystemStream diagnosticsSystemStream = new SystemStream("kafka", "test stream");

  private String jobName = "Testjob";
  private String jobId = "test job id";
  private String executionEnvContainerId = "exec container id";
  private String taskClassVersion = "0.0.1";
  private String samzaVersion = "1.3.0";
  private String hostname = "sample host name";
  private int containerMb = 1024;
  private int containerThreadPoolSize = 2;
  private long maxHeapSize = 900;
  private int numPersistentStores = 2;
  private int containerNumCores = 2;
  private boolean autosizingEnabled = false;
  private String deploymentType = "test deployment type";
  private String apiType = "test api type";
  private int numContainers = 1;
  private boolean hostAffinityEnabled = false;
  private String sspGrouperFactory = "org.apache.samza.container.grouper.stream.GroupByPartitionFactory";
  private int containerRetryCount = 8;
  private long containerRetryWindowMs = 300000;
  private int maxConcurrency = 1;
  private Map<String, ContainerModel> containerModels = TestDiagnosticsStreamMessage.getSampleContainerModels();
  private Collection<DiagnosticsExceptionEvent> exceptionEventList = TestDiagnosticsStreamMessage.getExceptionList();

  @Before
  public void setup() {

    // Mocked system producer for publishing to diagnostics stream
    mockSystemProducer = new MockSystemProducer();

    // Mocked scheduled executor service which does a synchronous run() on scheduling
    mockExecutorService = Mockito.mock(ScheduledExecutorService.class);
    Mockito.when(mockExecutorService.scheduleWithFixedDelay(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(),
        Mockito.eq(TimeUnit.SECONDS))).thenAnswer(invocation -> {
          ((Runnable) invocation.getArguments()[0]).run();
          return Mockito
              .mock(ScheduledFuture.class);
        });

    this.diagnosticsManager =
        new DiagnosticsManager(jobName, jobId, containerModels, containerMb, containerNumCores, numPersistentStores, maxHeapSize,
            containerThreadPoolSize, "0", executionEnvContainerId, taskClassVersion, samzaVersion, hostname, diagnosticsSystemStream,
            mockSystemProducer, Duration.ofSeconds(1), mockExecutorService, autosizingEnabled, deploymentType, apiType, numContainers,
            hostAffinityEnabled, sspGrouperFactory, containerRetryCount, containerRetryWindowMs, maxConcurrency);

    exceptionEventList.forEach(
      diagnosticsExceptionEvent -> this.diagnosticsManager.addExceptionEvent(diagnosticsExceptionEvent));

    this.diagnosticsManager.addProcessorStopEvent("0", executionEnvContainerId, hostname, 101);
  }

  @Test
  public void testDiagnosticsManagerStart() {
    SystemProducer mockSystemProducer = Mockito.mock(SystemProducer.class);
    DiagnosticsManager diagnosticsManager =
        new DiagnosticsManager(jobName, jobId, containerModels, containerMb, containerNumCores, numPersistentStores,
            maxHeapSize, containerThreadPoolSize, "0", executionEnvContainerId, taskClassVersion, samzaVersion,
            hostname, diagnosticsSystemStream, mockSystemProducer, Duration.ofSeconds(1), mockExecutorService,
            autosizingEnabled, deploymentType, apiType, numContainers,
            hostAffinityEnabled, sspGrouperFactory, containerRetryCount, containerRetryWindowMs, maxConcurrency);

    diagnosticsManager.start();

    Mockito.verify(mockSystemProducer, Mockito.times(1)).start();
    Mockito.verify(mockExecutorService, Mockito.times(1))
        .scheduleWithFixedDelay(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.anyLong(),
            Mockito.any(TimeUnit.class));
  }

  @Test
  public void testDiagnosticsManagerStop() throws InterruptedException {
    SystemProducer mockSystemProducer = Mockito.mock(SystemProducer.class);
    Mockito.when(mockExecutorService.isTerminated()).thenReturn(true);
    Duration terminationDuration = Duration.ofSeconds(1);
    DiagnosticsManager diagnosticsManager =
        new DiagnosticsManager(jobName, jobId, containerModels, containerMb, containerNumCores, numPersistentStores,
            maxHeapSize, containerThreadPoolSize, "0", executionEnvContainerId, taskClassVersion, samzaVersion,
            hostname, diagnosticsSystemStream, mockSystemProducer, terminationDuration, mockExecutorService,
            autosizingEnabled, deploymentType, apiType, numContainers,
            hostAffinityEnabled, sspGrouperFactory, containerRetryCount, containerRetryWindowMs, maxConcurrency);

    diagnosticsManager.stop();

    Mockito.verify(mockExecutorService, Mockito.times(1)).shutdown();
    Mockito.verify(mockExecutorService, Mockito.times(1))
        .awaitTermination(terminationDuration.toMillis(), TimeUnit.MILLISECONDS);
    Mockito.verify(mockExecutorService, Mockito.never()).shutdownNow();
    Mockito.verify(mockSystemProducer, Mockito.times(1)).stop();
  }

  @Test
  public void testDiagnosticsManagerForceStop() throws InterruptedException {
    SystemProducer mockSystemProducer = Mockito.mock(SystemProducer.class);
    Mockito.when(mockExecutorService.isTerminated()).thenReturn(false);
    Duration terminationDuration = Duration.ofSeconds(1);
    DiagnosticsManager diagnosticsManager =
        new DiagnosticsManager(jobName, jobId, containerModels, containerMb, containerNumCores, numPersistentStores,
            maxHeapSize, containerThreadPoolSize, "0", executionEnvContainerId, taskClassVersion, samzaVersion,
            hostname, diagnosticsSystemStream, mockSystemProducer, terminationDuration, mockExecutorService,
            autosizingEnabled, deploymentType, apiType, numContainers,
            hostAffinityEnabled, sspGrouperFactory, containerRetryCount, containerRetryWindowMs, maxConcurrency);

    diagnosticsManager.stop();

    Mockito.verify(mockExecutorService, Mockito.times(1)).shutdown();
    Mockito.verify(mockExecutorService, Mockito.times(1))
        .awaitTermination(terminationDuration.toMillis(), TimeUnit.MILLISECONDS);
    Mockito.verify(mockExecutorService, Mockito.times(1)).shutdownNow();
    Mockito.verify(mockSystemProducer, Mockito.times(1)).stop();
  }

  @Test
  public void testDiagnosticsStreamFirstMessagePublish() {
    // invoking start will do a syncrhonous publish to the stream because of our mocked scheduled exec service
    this.diagnosticsManager.start();
    Assert.assertEquals("One message should have been published", 1, mockSystemProducer.getEnvelopeList().size());
    OutgoingMessageEnvelope outgoingMessageEnvelope = mockSystemProducer.getEnvelopeList().get(0);
    validateOutgoingMessageEnvelope(outgoingMessageEnvelope);
  }

  @Test
  public void testNoDualPublish() {
    // Across two successive run() invocations only a single message should be published
    this.diagnosticsManager.start();
    this.diagnosticsManager.start();

    Assert.assertEquals("One message should have been published", 1, mockSystemProducer.getEnvelopeList().size());
    OutgoingMessageEnvelope outgoingMessageEnvelope = mockSystemProducer.getEnvelopeList().get(0);
    validateMetricsHeader(outgoingMessageEnvelope);
    validateOutgoingMessageEnvelope(outgoingMessageEnvelope);
  }

  @Test
  public void testSecondPublishWithProcessorStopInSecondMessage() {
    // Across two successive run() invocations two messages should be published if stop events are added
    this.diagnosticsManager.start();
    this.diagnosticsManager.addProcessorStopEvent("0", executionEnvContainerId, hostname, 102);
    this.diagnosticsManager.start();

    Assert.assertEquals("Two messages should have been published", 2, mockSystemProducer.getEnvelopeList().size());

    // Validate the first message
    OutgoingMessageEnvelope outgoingMessageEnvelope = mockSystemProducer.getEnvelopeList().get(0);
    validateMetricsHeader(outgoingMessageEnvelope);
    validateOutgoingMessageEnvelope(outgoingMessageEnvelope);

    // Validate the second message's header
    outgoingMessageEnvelope = mockSystemProducer.getEnvelopeList().get(1);
    validateMetricsHeader(outgoingMessageEnvelope);

    // Validate the second message's body (should be all empty except for the processor-stop-event)
    MetricsSnapshot metricsSnapshot =
        new MetricsSnapshotSerdeV2().fromBytes((byte[]) outgoingMessageEnvelope.getMessage());
    DiagnosticsStreamMessage diagnosticsStreamMessage =
        DiagnosticsStreamMessage.convertToDiagnosticsStreamMessage(metricsSnapshot);

    Assert.assertNull(diagnosticsStreamMessage.getContainerMb());
    Assert.assertNull(diagnosticsStreamMessage.getExceptionEvents());
    Assert.assertEquals(diagnosticsStreamMessage.getProcessorStopEvents(),
        Arrays.asList(new ProcessorStopEvent("0", executionEnvContainerId, hostname, 102)));
    Assert.assertNull(diagnosticsStreamMessage.getContainerModels());
    Assert.assertNull(diagnosticsStreamMessage.getContainerNumCores());
    Assert.assertNull(diagnosticsStreamMessage.getNumPersistentStores());
  }

  @Test
  public void testSecondPublishWithExceptionInSecondMessage() {
    // Across two successive run() invocations two messages should be published if stop events are added
    this.diagnosticsManager.start();
    DiagnosticsExceptionEvent diagnosticsExceptionEvent = new DiagnosticsExceptionEvent(System.currentTimeMillis(), new RuntimeException("exception"), new HashMap());
    this.diagnosticsManager.addExceptionEvent(diagnosticsExceptionEvent);
    this.diagnosticsManager.start();

    Assert.assertEquals("Two messages should have been published", 2, mockSystemProducer.getEnvelopeList().size());

    // Validate the first message
    OutgoingMessageEnvelope outgoingMessageEnvelope = mockSystemProducer.getEnvelopeList().get(0);
    validateMetricsHeader(outgoingMessageEnvelope);
    validateOutgoingMessageEnvelope(outgoingMessageEnvelope);

    // Validate the second message's header
    outgoingMessageEnvelope = mockSystemProducer.getEnvelopeList().get(1);
    validateMetricsHeader(outgoingMessageEnvelope);

    // Validate the second message's body (should be all empty except for the processor-stop-event)
    MetricsSnapshot metricsSnapshot =
        new MetricsSnapshotSerdeV2().fromBytes((byte[]) outgoingMessageEnvelope.getMessage());
    DiagnosticsStreamMessage diagnosticsStreamMessage =
        DiagnosticsStreamMessage.convertToDiagnosticsStreamMessage(metricsSnapshot);

    Assert.assertNull(diagnosticsStreamMessage.getContainerMb());
    Assert.assertEquals(Arrays.asList(diagnosticsExceptionEvent), diagnosticsStreamMessage.getExceptionEvents());
    Assert.assertNull(diagnosticsStreamMessage.getProcessorStopEvents());
    Assert.assertNull(diagnosticsStreamMessage.getContainerModels());
    Assert.assertNull(diagnosticsStreamMessage.getContainerNumCores());
    Assert.assertNull(diagnosticsStreamMessage.getNumPersistentStores());
  }

  @After
  public void teardown() throws Exception {
    this.diagnosticsManager.stop();
  }

  private void validateMetricsHeader(OutgoingMessageEnvelope outgoingMessageEnvelope) {
    // Validate the outgoing message

    Assert.assertTrue(outgoingMessageEnvelope.getSystemStream().equals(diagnosticsSystemStream));
    MetricsSnapshot metricsSnapshot =
        new MetricsSnapshotSerdeV2().fromBytes((byte[]) outgoingMessageEnvelope.getMessage());

    // Validate all header fields
    Assert.assertEquals(metricsSnapshot.getHeader().getJobName(), jobName);
    Assert.assertEquals(metricsSnapshot.getHeader().getJobId(), jobId);
    Assert.assertEquals(metricsSnapshot.getHeader().getExecEnvironmentContainerId(), executionEnvContainerId);
    Assert.assertEquals(metricsSnapshot.getHeader().getVersion(), taskClassVersion);
    Assert.assertEquals(metricsSnapshot.getHeader().getSamzaVersion(), samzaVersion);
    Assert.assertEquals(metricsSnapshot.getHeader().getHost(), hostname);
    Assert.assertEquals(metricsSnapshot.getHeader().getSource(), DiagnosticsManager.class.getName());
    Assert.assertEquals(metricsSnapshot.getHeader().getDeploymentType(), deploymentType);
    Assert.assertEquals(metricsSnapshot.getHeader().getApiType(), apiType);
    Assert.assertEquals(metricsSnapshot.getHeader().getNumContainers(), numContainers);
    Assert.assertEquals(metricsSnapshot.getHeader().getContainerMemoryMb(), containerMb);
    Assert.assertEquals(metricsSnapshot.getHeader().getContainerCpuCores(), containerNumCores);
    Assert.assertEquals(metricsSnapshot.getHeader().getContainerThreadPoolSize(), containerThreadPoolSize);
    Assert.assertEquals(metricsSnapshot.getHeader().getHostAffinity(), hostAffinityEnabled);
    Assert.assertEquals(metricsSnapshot.getHeader().getSspGrouper(), sspGrouperFactory);
    Assert.assertEquals(metricsSnapshot.getHeader().getMaxContainerRetryCount(), containerRetryCount);
    Assert.assertEquals(metricsSnapshot.getHeader().getContainerRetryWindowMs(), containerRetryWindowMs);
    Assert.assertEquals(metricsSnapshot.getHeader().getTaskMaxConcurrency(), maxConcurrency);
  }

  private void validateOutgoingMessageEnvelope(OutgoingMessageEnvelope outgoingMessageEnvelope) {
    MetricsSnapshot metricsSnapshot =
        new MetricsSnapshotSerdeV2().fromBytes((byte[]) outgoingMessageEnvelope.getMessage());

    // Validate the diagnostics stream message
    DiagnosticsStreamMessage diagnosticsStreamMessage =
        DiagnosticsStreamMessage.convertToDiagnosticsStreamMessage(metricsSnapshot);

    Assert.assertEquals(containerMb, diagnosticsStreamMessage.getContainerMb().intValue());
    Assert.assertEquals(maxHeapSize, diagnosticsStreamMessage.getMaxHeapSize().longValue());
    Assert.assertEquals(containerThreadPoolSize, diagnosticsStreamMessage.getContainerThreadPoolSize().intValue());
    Assert.assertEquals(exceptionEventList, diagnosticsStreamMessage.getExceptionEvents());
    Assert.assertEquals(diagnosticsStreamMessage.getProcessorStopEvents(), Arrays.asList(new ProcessorStopEvent("0", executionEnvContainerId, hostname, 101)));
    Assert.assertEquals(containerModels, diagnosticsStreamMessage.getContainerModels());
    Assert.assertEquals(containerNumCores, diagnosticsStreamMessage.getContainerNumCores().intValue());
    Assert.assertEquals(numPersistentStores, diagnosticsStreamMessage.getNumPersistentStores().intValue());
    Assert.assertEquals(autosizingEnabled, diagnosticsStreamMessage.getAutosizingEnabled());
  }

  private class MockSystemProducer implements SystemProducer {

    private final List<OutgoingMessageEnvelope> envelopeList = new ArrayList<>();

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void register(String source) {

    }

    @Override
    public void send(String source, OutgoingMessageEnvelope envelope) {
      envelopeList.add(envelope);
    }

    @Override
    public void flush(String source) {

    }

    public List<OutgoingMessageEnvelope> getEnvelopeList() {
      return this.envelopeList;
    }
  }
}
