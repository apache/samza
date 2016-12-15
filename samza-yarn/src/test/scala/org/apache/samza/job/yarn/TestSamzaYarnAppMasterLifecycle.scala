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

import java.net.URL
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.samza.SamzaException
import org.apache.samza.clustermanager.SamzaApplicationState
import org.apache.samza.clustermanager.SamzaApplicationState.SamzaAppStatus
import org.apache.samza.coordinator.JobModelManager
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito

class TestSamzaYarnAppMasterLifecycle {
  val coordinator = new JobModelManager(null, null)
  val amClient = new AMRMClientAsyncImpl[ContainerRequest](1, Mockito.mock(classOf[CallbackHandler])) {
    var host = ""
    var port = 0
    var status: FinalApplicationStatus = null
    override def registerApplicationMaster(appHostName: String, appHostPort: Int, appTrackingUrl: String): RegisterApplicationMasterResponse = {
      this.host = appHostName
      this.port = appHostPort
      new RegisterApplicationMasterResponse {
        override def setApplicationACLs(map: java.util.Map[ApplicationAccessType, String]): Unit = ()
        override def getApplicationACLs = null
        override def setMaximumResourceCapability(r: Resource): Unit = ()
        override def getMaximumResourceCapability = new Resource {
          def getMemory = 512
          def getVirtualCores = 2
          def setMemory(memory: Int) {}
          def setVirtualCores(vCores: Int) {}
          def compareTo(o: Resource) = 0
        }
        override def getClientToAMTokenMasterKey = null
        override def setClientToAMTokenMasterKey(buffer: ByteBuffer) {}
        override def getContainersFromPreviousAttempts(): java.util.List[Container] = java.util.Collections.emptyList[Container]
        override def getNMTokensFromPreviousAttempts(): java.util.List[NMToken] = java.util.Collections.emptyList[NMToken]
        override def getQueue(): String = null
        override def setContainersFromPreviousAttempts(containers: java.util.List[Container]): Unit = Unit
        override def setNMTokensFromPreviousAttempts(nmTokens: java.util.List[NMToken]): Unit = Unit
        override def setQueue(queue: String): Unit = Unit

        override def setSchedulerResourceTypes(types: java.util.EnumSet[SchedulerResourceTypes]): Unit = {}
        override def getSchedulerResourceTypes: java.util.EnumSet[SchedulerResourceTypes] = null
      }
    }
    override def unregisterApplicationMaster(appStatus: FinalApplicationStatus,
      appMessage: String,
      appTrackingUrl: String) {
      this.status = appStatus
    }
    override def releaseAssignedContainer(containerId: ContainerId) {}
    override def getClusterNodeCount() = 1

    override def serviceInit(config: Configuration) {}
    override def serviceStart() {}
    override def serviceStop() {}
  }

  @Test
  def testLifecycleShouldRegisterOnInit {
    val state = new SamzaApplicationState(coordinator)

    val yarnState = new YarnAppState(1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "testHost", 1, 2);
    yarnState.rpcUrl = new URL("http://localhost:1")
    yarnState.trackingUrl = new URL("http://localhost:2")

    val saml = new SamzaYarnAppMasterLifecycle(512, 2, state, yarnState, amClient)
    saml.onInit
    assertEquals("testHost", amClient.host)
    assertEquals(1, amClient.port)
    assertFalse(saml.shouldShutdown)
  }

  @Test
  def testLifecycleShouldUnregisterOnShutdown {
    val state = new SamzaApplicationState(coordinator)

    val yarnState =  new YarnAppState(1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "testHost", 1, 2);
    new SamzaYarnAppMasterLifecycle(512, 2, state, yarnState, amClient).onShutdown (SamzaAppStatus.SUCCEEDED)
    assertEquals(FinalApplicationStatus.SUCCEEDED, amClient.status)
  }

  @Test
  def testLifecycleShouldThrowAnExceptionOnReboot {
    var gotException = false
    try {
      val state = new SamzaApplicationState(coordinator)

      val yarnState =  new YarnAppState(1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "testHost", 1, 2);
      new SamzaYarnAppMasterLifecycle(512, 2, state, yarnState, amClient).onReboot()
    } catch {
      // expected
      case e: SamzaException => gotException = true
    }
    assertTrue(gotException)
  }

  @Test
  def testLifecycleShouldShutdownOnInvalidContainerSettings {
    val state = new SamzaApplicationState(coordinator)

    val yarnState =  new YarnAppState(1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "testHost", 1, 2);
    yarnState.rpcUrl = new URL("http://localhost:1")
    yarnState.trackingUrl = new URL("http://localhost:2")

    //Request a higher amount of memory from yarn.
    List(new SamzaYarnAppMasterLifecycle(768, 1, state, yarnState, amClient),
    //Request a higher number of cores from yarn.
      new SamzaYarnAppMasterLifecycle(368, 3, state, yarnState, amClient)).map(saml => {
        saml.onInit
        assertTrue(saml.shouldShutdown)
      })
  }
}
