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
package org.apache.samza.execution;

import java.util.Collections;
import java.util.List;
import org.apache.samza.application.AppDescriptorImpl;
import org.apache.samza.application.StreamAppDescriptorImpl;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.system.StreamSpec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Unit tests for {@link RemoteJobPlanner}
 *
 * TODO: consolidate this with unit tests for ExecutionPlanner after SAMZA-1811
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(RemoteJobPlanner.class)
public class TestRemoteJobPlanner {

  private RemoteJobPlanner remotePlanner;

  @Test
  public void testStreamCreation()
      throws Exception {
    remotePlanner = createRemoteJobPlanner(mock(StreamAppDescriptorImpl.class));
    StreamManager streamManager = mock(StreamManager.class);
    doReturn(streamManager).when(remotePlanner).buildAndStartStreamManager(any(Config.class));

    ExecutionPlan plan = mock(ExecutionPlan.class);
    when(plan.getIntermediateStreams()).thenReturn(
        Collections.singletonList(new StreamSpec("test-stream", "test-stream", "test-system")));
    when(plan.getPlanAsJson()).thenReturn("");
    when(plan.getJobConfigs()).thenReturn(Collections.singletonList(mock(JobConfig.class)));
    ApplicationConfig mockAppConfig = mock(ApplicationConfig.class);
    when(mockAppConfig.getAppMode()).thenReturn(ApplicationConfig.ApplicationMode.STREAM);
    when(plan.getApplicationConfig()).thenReturn(mockAppConfig);
    doReturn(plan).when(remotePlanner).getExecutionPlan(any(), any());

    remotePlanner.prepareJobs();

    verify(streamManager, times(0)).clearStreamsFromPreviousRun(any());
    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
    verify(streamManager).createStreams(captor.capture());
    List<StreamSpec> streamSpecs = captor.getValue();
    assertEquals(streamSpecs.size(), 1);
    assertEquals(streamSpecs.get(0).getId(), "test-stream");
    verify(streamManager).stop();
  }

  private RemoteJobPlanner createRemoteJobPlanner(AppDescriptorImpl appDesc) {
    return spy(new RemoteJobPlanner(appDesc));
  }
}
