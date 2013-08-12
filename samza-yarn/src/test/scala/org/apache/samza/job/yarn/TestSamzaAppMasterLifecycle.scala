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

package org.apache.samza.job.yarn
import org.junit.Assert._
import org.junit.Test
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.client.AMRMClient
import org.apache.hadoop.yarn.client.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.service._
import org.apache.samza.SamzaException
import org.apache.hadoop.yarn.api.records.ApplicationAccessType

class TestSamzaAppMasterLifecycle {
  val amClient = new AMRMClient {
    var host = ""
    var port = 0
    var status: FinalApplicationStatus = null
    def registerApplicationMaster(appHostName: String, appHostPort: Int, appTrackingUrl: String): RegisterApplicationMasterResponse = {
      this.host = appHostName
      this.port = appHostPort
      new RegisterApplicationMasterResponse {
        def setApplicationACLs(map: java.util.Map[ApplicationAccessType, String]) = null
        def getApplicationACLs = null
        def setMaximumResourceCapability(r: Resource) = null
        def getMaximumResourceCapability = new Resource {
          def getMemory = 512
          def getVirtualCores = 2
          def setMemory(memory: Int) {}
          def setVirtualCores(vCores: Int) {}
          def compareTo(o: Resource) = 0
        }
        def setMinimumResourceCapability(r: Resource) = null
        def getMinimumResourceCapability = new Resource {
          def getMemory = 128
          def getVirtualCores = 1
          def setMemory(memory: Int) {}
          def setVirtualCores(vCores: Int) {}
          def compareTo(o: Resource) = 0
        }
      }
    }
    def allocate(progressIndicator: Float): AllocateResponse = null
    def unregisterApplicationMaster(appStatus: FinalApplicationStatus,
      appMessage: String,
      appTrackingUrl: String) {
      this.status = appStatus
    }
    def addContainerRequest(req: ContainerRequest) {}
    def removeContainerRequest(req: ContainerRequest) {}
    def releaseAssignedContainer(containerId: ContainerId) {}
    def getClusterAvailableResources(): Resource = null
    def getClusterNodeCount() = 1

    def init(config: Configuration) {}
    def start() {}
    def stop() {}
    def register(listener: ServiceStateChangeListener) {}
    def unregister(listener: ServiceStateChangeListener) {}
    def getName(): String = ""
    def getConfig() = null
    def getServiceState() = null
    def getStartTime() = 0L
  }

  @Test
  def testLifecycleShouldRegisterOnInit {
    val state = new SamzaAppMasterState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "test", 1, 2)
    state.rpcPort = 1
    val saml = new SamzaAppMasterLifecycle(512, 2, state, amClient, new YarnConfiguration)
    saml.onInit
    assert(amClient.host == "test")
    assert(amClient.port == 1)
    assertFalse(saml.shouldShutdown)
  }

  @Test
  def testLifecycleShouldUnregisterOnShutdown {
    val state = new SamzaAppMasterState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "", 1, 2)
    state.status = FinalApplicationStatus.SUCCEEDED
    new SamzaAppMasterLifecycle(128, 1, state, amClient, new YarnConfiguration).onShutdown
    assert(amClient.status == FinalApplicationStatus.SUCCEEDED)
  }

  @Test
  def testLifecycleShouldThrowAnExceptionOnReboot {
    var gotException = false
    try {
      new SamzaAppMasterLifecycle(368, 1, null, amClient, new YarnConfiguration).onReboot
    } catch {
      // expected
      case e: SamzaException => gotException = true
    }
    assert(gotException)
  }

  @Test
  def testLifecycleShouldShutdownOnInvalidContainerSettings {
    val state = new SamzaAppMasterState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "test", 1, 2)
    state.rpcPort = 1
    List(new SamzaAppMasterLifecycle(768, 1, state, amClient, new YarnConfiguration),
      new SamzaAppMasterLifecycle(0, 1, state, amClient, new YarnConfiguration),
      new SamzaAppMasterLifecycle(368, 3, state, amClient, new YarnConfiguration),
      new SamzaAppMasterLifecycle(768, 0, state, amClient, new YarnConfiguration)).map(saml => {
        saml.onInit
        assertTrue(saml.shouldShutdown)
      })
  }
}
