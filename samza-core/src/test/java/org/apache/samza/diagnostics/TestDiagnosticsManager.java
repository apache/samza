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

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Clock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class TestDiagnosticsManager {
  private DiagnosticsManager diagnosticsManager;
  private MockSystemProducer mockSystemProducer;
  private ScheduledExecutorService mockExecutorService;
  private final SystemStream diagnosticsSystemStream = new SystemStream("kafka", "test stream");

  private static final String JOB_NAME = "Testjob";
  private static final String JOB_ID = "test job id";
  private static final String EXECUTION_ENV_CONTAINER_ID = "exec container id";
  private static final String SAMZA_EPOCH_ID = "epoch-123";
  private static final String TASK_CLASS_VERSION = "0.0.1";
  private static final String SAMZA_VERSION = "1.3.0";
  private static final String HOSTNAME = "sample host name";
  private static final int CONTAINER_MB = 1024;
  private static final int CONTAINER_THREAD_POOL_SIZE = 2;
  private static final long MAX_HEAP_SIZE = 900;
  private static final int NUM_PERSISTENT_STORES = 2;
  private static final int CONTAINER_NUM_CORES = 2;
  private static final boolean AUTOSIZING_ENABLED = false;
  private static final long RESET_TIME = 10;
  private static final long FIRST_SEND_TIME = 20;
  private static final long SECOND_SEND_TIME = 30;
  private final Config config = new MapConfig(ImmutableMap.of("job.name", JOB_NAME, "job.id", JOB_ID,
      "cluster-manager.container.memory.mb", "1024", "cluster-manager.container. cpu.cores", "1",
      "cluster-manager.container.retry.count", "8"));
  private final Map<String, ContainerModel> containerModels = TestDiagnosticsStreamMessage.getSampleContainerModels();
  private final Collection<DiagnosticsExceptionEvent> exceptionEventList =
      TestDiagnosticsStreamMessage.getExceptionList();
  private Clock clock;

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

    this.clock = Mockito.mock(Clock.class);
    // first call is for getting reset time, then other calls are for timestamp when message is being published
    Mockito.when(this.clock.currentTimeMillis()).thenReturn(RESET_TIME, FIRST_SEND_TIME, SECOND_SEND_TIME);

    this.diagnosticsManager =
        new DiagnosticsManager(JOB_NAME, JOB_ID, containerModels, CONTAINER_MB, CONTAINER_NUM_CORES,
            NUM_PERSISTENT_STORES, MAX_HEAP_SIZE, CONTAINER_THREAD_POOL_SIZE, "0", EXECUTION_ENV_CONTAINER_ID,
            SAMZA_EPOCH_ID, TASK_CLASS_VERSION, SAMZA_VERSION, HOSTNAME, diagnosticsSystemStream,
            mockSystemProducer, Duration.ofSeconds(1), mockExecutorService, AUTOSIZING_ENABLED, config, this.clock);

    exceptionEventList.forEach(
      diagnosticsExceptionEvent -> this.diagnosticsManager.addExceptionEvent(diagnosticsExceptionEvent));

    this.diagnosticsManager.addProcessorStopEvent("0", EXECUTION_ENV_CONTAINER_ID, HOSTNAME, 101);
  }

  @Test
  public void testDiagnosticsManagerStart() {
    SystemProducer mockSystemProducer = Mockito.mock(SystemProducer.class);
    DiagnosticsManager diagnosticsManager =
        new DiagnosticsManager(JOB_NAME, JOB_ID, containerModels, CONTAINER_MB, CONTAINER_NUM_CORES,
            NUM_PERSISTENT_STORES, MAX_HEAP_SIZE, CONTAINER_THREAD_POOL_SIZE, "0", EXECUTION_ENV_CONTAINER_ID,
            SAMZA_EPOCH_ID, TASK_CLASS_VERSION, SAMZA_VERSION, HOSTNAME, diagnosticsSystemStream,
            mockSystemProducer, Duration.ofSeconds(1), mockExecutorService, AUTOSIZING_ENABLED, config, this.clock);

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
        new DiagnosticsManager(JOB_NAME, JOB_ID, containerModels, CONTAINER_MB, CONTAINER_NUM_CORES,
            NUM_PERSISTENT_STORES, MAX_HEAP_SIZE, CONTAINER_THREAD_POOL_SIZE, "0", EXECUTION_ENV_CONTAINER_ID,
            SAMZA_EPOCH_ID, TASK_CLASS_VERSION, SAMZA_VERSION, HOSTNAME, diagnosticsSystemStream,
            mockSystemProducer, terminationDuration, mockExecutorService, AUTOSIZING_ENABLED, config, this.clock);

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
        new DiagnosticsManager(JOB_NAME, JOB_ID, containerModels, CONTAINER_MB, CONTAINER_NUM_CORES,
            NUM_PERSISTENT_STORES, MAX_HEAP_SIZE, CONTAINER_THREAD_POOL_SIZE, "0", EXECUTION_ENV_CONTAINER_ID,
            SAMZA_EPOCH_ID, TASK_CLASS_VERSION, SAMZA_VERSION, HOSTNAME, diagnosticsSystemStream,
            mockSystemProducer, terminationDuration, mockExecutorService, AUTOSIZING_ENABLED, config, this.clock);

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
    validateMetricsHeader(outgoingMessageEnvelope, FIRST_SEND_TIME);
    validateOutgoingMessageEnvelope(outgoingMessageEnvelope);
  }

  @Test
  public void testSecondPublishWithProcessorStopInSecondMessage() {
    // Across two successive run() invocations two messages should be published if stop events are added
    this.diagnosticsManager.start();
    this.diagnosticsManager.addProcessorStopEvent("0", EXECUTION_ENV_CONTAINER_ID, HOSTNAME, 102);
    this.diagnosticsManager.start();

    Assert.assertEquals("Two messages should have been published", 2, mockSystemProducer.getEnvelopeList().size());

    // Validate the first message
    OutgoingMessageEnvelope outgoingMessageEnvelope = mockSystemProducer.getEnvelopeList().get(0);
    validateMetricsHeader(outgoingMessageEnvelope, FIRST_SEND_TIME);
    validateOutgoingMessageEnvelope(outgoingMessageEnvelope);

    // Validate the second message's header
    outgoingMessageEnvelope = mockSystemProducer.getEnvelopeList().get(1);
    validateMetricsHeader(outgoingMessageEnvelope, SECOND_SEND_TIME);

    // Validate the second message's body (should be all empty except for the processor-stop-event)
    MetricsSnapshot metricsSnapshot =
        new MetricsSnapshotSerdeV2().fromBytes((byte[]) outgoingMessageEnvelope.getMessage());
    DiagnosticsStreamMessage diagnosticsStreamMessage =
        DiagnosticsStreamMessage.convertToDiagnosticsStreamMessage(metricsSnapshot);

    Assert.assertNull(diagnosticsStreamMessage.getContainerMb());
    Assert.assertNull(diagnosticsStreamMessage.getExceptionEvents());
    Assert.assertEquals(diagnosticsStreamMessage.getProcessorStopEvents(),
        Arrays.asList(new ProcessorStopEvent("0", EXECUTION_ENV_CONTAINER_ID, HOSTNAME, 102)));
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
    validateMetricsHeader(outgoingMessageEnvelope, FIRST_SEND_TIME);
    validateOutgoingMessageEnvelope(outgoingMessageEnvelope);

    // Validate the second message's header
    outgoingMessageEnvelope = mockSystemProducer.getEnvelopeList().get(1);
    validateMetricsHeader(outgoingMessageEnvelope, SECOND_SEND_TIME);

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

  private void validateMetricsHeader(OutgoingMessageEnvelope outgoingMessageEnvelope, long sendTime) {
    // Validate the outgoing message

    Assert.assertTrue(outgoingMessageEnvelope.getSystemStream().equals(diagnosticsSystemStream));
    MetricsSnapshot metricsSnapshot =
        new MetricsSnapshotSerdeV2().fromBytes((byte[]) outgoingMessageEnvelope.getMessage());

    MetricsHeader expectedHeader = new MetricsHeader(JOB_NAME, JOB_ID, "samza-container-0", EXECUTION_ENV_CONTAINER_ID,
        Optional.of(SAMZA_EPOCH_ID), DiagnosticsManager.class.getName(), TASK_CLASS_VERSION, SAMZA_VERSION,
        HOSTNAME, sendTime, RESET_TIME);
    Assert.assertEquals(expectedHeader, metricsSnapshot.getHeader());
  }

  private void validateOutgoingMessageEnvelope(OutgoingMessageEnvelope outgoingMessageEnvelope) {
    MetricsSnapshot metricsSnapshot =
        new MetricsSnapshotSerdeV2().fromBytes((byte[]) outgoingMessageEnvelope.getMessage());

    // Validate the diagnostics stream message
    DiagnosticsStreamMessage diagnosticsStreamMessage =
        DiagnosticsStreamMessage.convertToDiagnosticsStreamMessage(metricsSnapshot);

    Assert.assertEquals(CONTAINER_MB, diagnosticsStreamMessage.getContainerMb().intValue());
    Assert.assertEquals(MAX_HEAP_SIZE, diagnosticsStreamMessage.getMaxHeapSize().longValue());
    Assert.assertEquals(CONTAINER_THREAD_POOL_SIZE, diagnosticsStreamMessage.getContainerThreadPoolSize().intValue());
    Assert.assertEquals(exceptionEventList, diagnosticsStreamMessage.getExceptionEvents());
    Assert.assertEquals(diagnosticsStreamMessage.getProcessorStopEvents(), Arrays.asList(new ProcessorStopEvent("0",
        EXECUTION_ENV_CONTAINER_ID, HOSTNAME, 101)));
    Assert.assertEquals(containerModels, diagnosticsStreamMessage.getContainerModels());
    Assert.assertEquals(CONTAINER_NUM_CORES, diagnosticsStreamMessage.getContainerNumCores().intValue());
    Assert.assertEquals(NUM_PERSISTENT_STORES, diagnosticsStreamMessage.getNumPersistentStores().intValue());
    Assert.assertEquals(AUTOSIZING_ENABLED, diagnosticsStreamMessage.getAutosizingEnabled());
    Assert.assertEquals(config, diagnosticsStreamMessage.getConfig());
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
