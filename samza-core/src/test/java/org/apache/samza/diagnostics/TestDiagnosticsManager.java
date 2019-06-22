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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class TestDiagnosticsManager {
  private DiagnosticsManager diagnosticsManager;
  private MockCoordinatorStreamSystemFactory.MockSystemProducer mockSystemProducer;
  private SystemStream diagnosticsSystemStream = new SystemStream("kafka", "test stream");

  private String jobName = "Testjob";
  private String jobId = "test job id";
  private String executionEnvContainerId = "exec container id";
  private String taskClassVersion = "0.0.1";
  private String samzaVersion = "1.3.0";
  private String hostname = "sample host name";
  private int containerMb = 1024;
  private int numStoresWithChangelog = 2;
  private int containerNumCores = 2;
  private Map<String, ContainerModel> containerModels = TestDiagnosticsStreamMessage.getSampleContainerModels();
  private Collection<DiagnosticsExceptionEvent> exceptionEventList = TestDiagnosticsStreamMessage.getExceptionList();

  @Before
  public void setup() {

    // Mocked system producer for publishing to diagnostics stream
    mockSystemProducer = new MockCoordinatorStreamSystemFactory.MockSystemProducer("source");

    // Mocked scheduled executor service which does a synchronous run() on scheduling
    ScheduledExecutorService mockExecutorService = Mockito.mock(ScheduledExecutorService.class);
    Mockito.when(mockExecutorService.scheduleWithFixedDelay(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(),
             Mockito.eq(TimeUnit.SECONDS))).thenAnswer(invocation -> {
                 ((Runnable) invocation.getArguments()[0]).run();
                 return Mockito.mock(ScheduledFuture.class);
               });

    this.diagnosticsManager =
        new DiagnosticsManager(jobName, jobId, containerModels, containerMb, containerNumCores, numStoresWithChangelog,
            "0", executionEnvContainerId, taskClassVersion, samzaVersion, hostname, diagnosticsSystemStream,
            mockSystemProducer, Duration.ofSeconds(1), mockExecutorService);

    exceptionEventList.forEach(
        diagnosticsExceptionEvent -> this.diagnosticsManager.addExceptionEvent(diagnosticsExceptionEvent));

    this.diagnosticsManager.addProcessorStopEvent("0", executionEnvContainerId, hostname, 101);
  }

  @Test
  public void testDiagnosticsStreamPublish() {
    // invoking start will do a syncrhonous publish to the stream because of our mocked scheduled exec service
    this.diagnosticsManager.start();
    Assert.assertEquals("One message should have been published", 1, mockSystemProducer.getEnvelopes().size());

    // Validate the outgoing message
    OutgoingMessageEnvelope outgoingMessageEnvelope = mockSystemProducer.getEnvelopes().get(0);
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

    // Validate the diagnostics stream message
    DiagnosticsStreamMessage diagnosticsStreamMessage =
        DiagnosticsStreamMessage.convertToDiagnosticsStreamMessage(metricsSnapshot);

    Assert.assertEquals(containerMb, diagnosticsStreamMessage.getContainerMb());
    Assert.assertTrue(diagnosticsStreamMessage.getExceptionEvents().equals(exceptionEventList));
    Assert.assertTrue(diagnosticsStreamMessage.getProcessorStopEvents()
        .equals(Arrays.asList(new ProcessorStopEvent("0", executionEnvContainerId, hostname, 101))));
    Assert.assertEquals(containerModels, diagnosticsStreamMessage.getContainerModels());
    Assert.assertEquals(containerNumCores, diagnosticsStreamMessage.getContainerNumCores());
    Assert.assertEquals(numStoresWithChangelog, diagnosticsStreamMessage.getNumStoresWithChangelog());
  }

  @After
  public void teardown() throws Exception {
    this.diagnosticsManager.stop();
  }
}
