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
package org.apache.samza.job.yarn.util;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.samza.config.Config;
import org.apache.samza.job.yarn.ContainerUtil;
import org.apache.samza.job.yarn.SamzaAppMaster$;
import org.apache.samza.job.yarn.SamzaAppState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestUtil {

  public static AMRMClientAsyncImpl<ContainerRequest> getAMClient(final TestAMRMClientImpl amClient) {
    return new AMRMClientAsyncImpl<ContainerRequest>(amClient, 1, SamzaAppMaster$.MODULE$) {
          public TestAMRMClientImpl getClient() {
            return amClient;
          }
    };
  }

  public static AllocateResponse getAppMasterResponse(final boolean reboot,
                                               final List<Container> containers,
                                               final List<ContainerStatus> completed) {
    return new AllocateResponse() {
      @Override
      public AMCommand getAMCommand() {
      // Not sure how to throw exception without changing method signature!
        if (reboot) {
          try {
            throw new ApplicationAttemptNotFoundException("Test - out of sync");
          } catch (ApplicationAttemptNotFoundException e) {
            return AMCommand.AM_RESYNC;
          }
        } else {
          return null;
        }
      }

      @Override
      public void setAMCommand(AMCommand command) {}

      @Override
      public int getResponseId() {
        return 0;
      }

      @Override
      public void setResponseId(int responseId) {}

      @Override
      public List<Container> getAllocatedContainers() {
        return containers;
      }

      @Override
      public void setAllocatedContainers(List<Container> containers) {}

      @Override
      public Resource getAvailableResources() {
        return null;
      }

      @Override
      public void setAvailableResources(Resource limit) {}

      @Override
      public List<ContainerStatus> getCompletedContainersStatuses() {
        return completed;
      }

      @Override
      public void setCompletedContainersStatuses(List<ContainerStatus> containers) {}

      @Override
      public List<NodeReport> getUpdatedNodes() {
        return new ArrayList<NodeReport>();
      }

      @Override
      public void setUpdatedNodes(List<NodeReport> updatedNodes) {}

      @Override
      public int getNumClusterNodes() {
        return 1;
      }

      @Override
      public void setNumClusterNodes(int numNodes) {

      }

      @Override
      public PreemptionMessage getPreemptionMessage() {
        return null;
      }

      @Override
      public void setPreemptionMessage(PreemptionMessage request) {}

      @Override
      public List<NMToken> getNMTokens() {
        return new ArrayList<NMToken>();
      }

      @Override
      public void setNMTokens(List<NMToken> nmTokens) {}

      @Override
      public List<ContainerResourceIncrease> getIncreasedContainers() {
        return Collections.<ContainerResourceIncrease>emptyList();
      }

      @Override
      public void setIncreasedContainers(List<ContainerResourceIncrease> increasedContainers) {}

      @Override
      public List<ContainerResourceDecrease> getDecreasedContainers() {
        return Collections.<ContainerResourceDecrease>emptyList();
      }

      @Override
      public void setDecreasedContainers(List<ContainerResourceDecrease> decreasedContainers) {

      }

      @Override
      public Token getAMRMToken() {
        return null;
      }

      @Override
      public void setAMRMToken(Token amRMToken) {}
    };
  }

  public static Container getContainer(final ContainerId containerId, final String host, final int port) {
    return new Container() {
      @Override
      public ContainerId getId() {
        return containerId;
      }

      @Override
      public void setId(ContainerId id) { }

      @Override
      public NodeId getNodeId() {
        return NodeId.newInstance(host, port);
      }

      @Override
      public void setNodeId(NodeId nodeId) {  }

      @Override
      public String getNodeHttpAddress() {
        return host + ":" + port;
      }

      @Override
      public void setNodeHttpAddress(String nodeHttpAddress) {  }

      @Override
      public Resource getResource() {
        return null;
      }

      @Override
      public void setResource(Resource resource) {  }

      @Override
      public Priority getPriority() {
        return null;
      }

      @Override
      public void setPriority(Priority priority) {  }

      @Override
      public Token getContainerToken() {
        return null;
      }

      @Override
      public void setContainerToken(Token containerToken) { }

      @Override
      public int compareTo(Container o) {
        return containerId.compareTo(o.getId());
      }
    };
  }

  /**
   * Returns MockContainerUtil instance with a Mock NMClient
   * */
  public static ContainerUtil getContainerUtil(Config config, SamzaAppState state) {
    return new MockContainerUtil(config, state, new YarnConfiguration(), new MockNMClient("Mock NMClient"));
  }

  public static ContainerStatus getContainerStatus(final ContainerId containerId,
                                                   final int exitCode,
                                                   final String diagnostic) {
    return new ContainerStatus() {
      @Override
      public ContainerId getContainerId() {
        return containerId;
      }

      @Override
      public void setContainerId(ContainerId containerId) { }

      @Override
      public ContainerState getState() {
        return null;
      }

      @Override
      public void setState(ContainerState state) {  }

      @Override
      public int getExitStatus() {
        return exitCode;
      }

      @Override
      public void setExitStatus(int exitStatus) { }

      @Override
      public String getDiagnostics() {
        return diagnostic;
      }

      @Override
      public void setDiagnostics(String diagnostics) {  }
    };
  }
}
