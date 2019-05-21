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

package org.apache.samza.job.yarn;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.samza.clustermanager.ClusterResourceManager;
import org.apache.samza.config.Config;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

public class TestYarnClusterResourceManager {

  @Test
  public void testErrorInStartContainerShouldUpdateState() {
    // create mocks
    final int samzaContainerId = 1;
    YarnConfiguration yarnConfiguration = mock(YarnConfiguration.class);
    SamzaAppMasterMetrics metrics = mock(SamzaAppMasterMetrics.class);
    Config config = mock(Config.class);
    AMRMClientAsync asyncClient = mock(AMRMClientAsync.class);
    YarnAppState yarnAppState = new YarnAppState(0, mock(ContainerId.class), "host", 8080, 8081);
    SamzaYarnAppMasterLifecycle lifecycle = mock(SamzaYarnAppMasterLifecycle.class);
    SamzaYarnAppMasterService service = mock(SamzaYarnAppMasterService.class);
    NMClientAsync asyncNMClient = mock(NMClientAsync.class);
    ClusterResourceManager.Callback callback = mock(ClusterResourceManager.Callback.class);

    // start the cluster manager
    YarnClusterResourceManager yarnClusterResourceManager = new YarnClusterResourceManager(asyncClient, asyncNMClient,
        callback, yarnAppState, lifecycle, service, metrics, yarnConfiguration, config);

    yarnAppState.pendingProcessors.put(String.valueOf(samzaContainerId),
        new YarnContainer(Container.newInstance(
            ContainerId.newContainerId(
                ApplicationAttemptId.newInstance(
                    ApplicationId.newInstance(10000l, 1), 1), 1),
            NodeId.newInstance("host1", 8088), "http://host1",
            Resource.newInstance(1024, 1), Priority.newInstance(1),
            Token.newInstance("id".getBytes(), "read", "password".getBytes(), "service"))));

    yarnClusterResourceManager.start();
    assertEquals(1, yarnAppState.pendingProcessors.size());

    yarnClusterResourceManager.onStartContainerError(ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(10000l, 1), 1), 1),
        new Exception());

    assertEquals(0, yarnAppState.pendingProcessors.size());
    verify(callback, times(1)).onStreamProcessorLaunchFailure(anyObject(), any(Exception.class));
  }
}