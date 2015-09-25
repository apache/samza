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

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse

import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.samza.job.yarn.util.{TestUtil, TestAMRMClientImpl}
import org.junit.Test
import org.junit.Assert._


class TestSamzaAppMaster {
  @Test
  def testAppMasterShouldShutdown {
    val amClient = TestUtil.getAMClient(
      new TestAMRMClientImpl(
        TestUtil.getAppMasterResponse(
          false,
          new java.util.ArrayList[Container](),
          new java.util.ArrayList[ContainerStatus]())
      ))
    val listener = new YarnAppMasterListener {
      var init = 0
      var shutdown = 0
      var allocated = 0
      var complete = 0
      override def shouldShutdown = true
      override def onInit() {
        init += 1
      }
      override def onShutdown() {
        shutdown += 1
      }
      override def onContainerAllocated(container: Container) {
        allocated += 1
      }
      override def onContainerCompleted(containerStatus: ContainerStatus) {
        complete += 1
      }
    }
    SamzaAppMaster.listeners = List(listener)
    SamzaAppMaster.run(amClient, SamzaAppMaster.listeners, new YarnConfiguration, 1)
    assertEquals(1, listener.init)
    assertEquals(1, listener.shutdown)
  }

  @Test
  def testAppMasterShouldShutdownWithFailingListener {
    val amClient = TestUtil.getAMClient(
      new TestAMRMClientImpl(
        TestUtil.getAppMasterResponse(
          false,
          new java.util.ArrayList[Container](),
          new java.util.ArrayList[ContainerStatus]())))
    val listener1 = new YarnAppMasterListener {
      var shutdown = 0
      override def shouldShutdown = true
      override def onShutdown() {
        shutdown += 1
        throw new RuntimeException("Some weird failure")
      }
    }
    val listener2 = new YarnAppMasterListener {
      var shutdown = 0
      override def shouldShutdown = true
      override def onShutdown() {
        shutdown += 1
      }
    }
    // listener1 will throw an exception in shutdown, and listener2 should still get called
    SamzaAppMaster.listeners = List(listener1, listener2)
    SamzaAppMaster.run(amClient, SamzaAppMaster.listeners, new YarnConfiguration, 1)
    assertEquals(1, listener1.shutdown)
    assertEquals(1, listener2.shutdown)
  }

  @Test
  def testAppMasterShouldShutdownWithInterrupt {
    val amClient = TestUtil.getAMClient(
      new TestAMRMClientImpl(
        TestUtil.getAppMasterResponse(
          false,
          new java.util.ArrayList[Container](),
          new java.util.ArrayList[ContainerStatus]())
      )
    )
    val listener = new YarnAppMasterListener {
      var init = 0
      var shutdown = 0
      override def shouldShutdown = false
      override def onInit() {
        init += 1
      }
      override def onShutdown() {
        shutdown += 1
      }
    }
    val thread = new Thread {
      override def run {
        SamzaAppMaster.listeners = List(listener)
        SamzaAppMaster.run(amClient, SamzaAppMaster.listeners, new YarnConfiguration, 1)
      }
    }
    thread.start
    thread.interrupt
    thread.join
    assertEquals(1, listener.init)
    assertEquals(1, listener.shutdown)
  }

  @Test
  def testAppMasterShouldForwardAllocatedAndCompleteContainers {
    val amClient = TestUtil.getAMClient(
      new TestAMRMClientImpl(
        TestUtil.getAppMasterResponse(
          false,
          new java.util.ArrayList[Container]{ add(TestUtil.getContainer(null, "", 12345));  },
          new java.util.ArrayList[ContainerStatus]{ add(TestUtil.getContainerStatus(null, 1, null));  })
      )
    )
    val listener = new YarnAppMasterListener {
      var allocated = 0
      var complete = 0
      override def onInit(): Unit = amClient.registerApplicationMaster("", -1, "")
      override def shouldShutdown = (allocated >= 1 && complete >= 1)
      override def onContainerAllocated(container: Container) {
        allocated += 1
      }
      override def onContainerCompleted(containerStatus: ContainerStatus) {
        complete += 1
      }
    }
    SamzaAppMaster.listeners = List(listener)
    SamzaAppMaster.run(amClient, SamzaAppMaster.listeners, new YarnConfiguration, 1)
    // heartbeat may be triggered for more than once
    assertTrue(listener.allocated >= 1)
    assertTrue(listener.complete >= 1)
  }

  @Test
  def testAppMasterShouldReboot {
    val response: AllocateResponse = getAppMasterResponse(
      true,
      new java.util.ArrayList[Container](),
      new java.util.ArrayList[ContainerStatus]())

    val amClient = TestUtil.getAMClient(
      new TestAMRMClientImpl(response))

    val listener = new YarnAppMasterListener {
      var reboot = 0
      override def onInit(): Unit = amClient.registerApplicationMaster("", -1, "")
      override def shouldShutdown = reboot >= 1
      override def onReboot() {
        reboot += 1
      }
    }
    SamzaAppMaster.listeners = List(listener)
    SamzaAppMaster.run(amClient, SamzaAppMaster.listeners, new YarnConfiguration, 1)
    // heartbeat may be triggered for more than once
    assertTrue(listener.reboot >= 1)
  }

  /**
   * This method is necessary because in Yarn 2.6, an RM reboot results in the allocate() method throwing an exception,
   * rather than invoking AM_RESYNC command. However, we cannot mock out the AllocateResponse class in java because it
   * will require the getAMCommand() signature to change and allow throwing an exception. This is however allowed in Scala.
   * Since this is beyond our scope and we don't have a better way to mock the scenario for an RM reboot in our unit
   * tests, we are keeping the following scala method for now.
   */
  def getAppMasterResponse(reboot: Boolean, containers: java.util.List[Container], completed: java.util.List[ContainerStatus]) =
    new AllocateResponse {
      override def getResponseId() = 0
      override def setResponseId(responseId: Int) {}
      override def getAllocatedContainers() = containers
      override def setAllocatedContainers(containers: java.util.List[Container]) {}
      override def getAvailableResources(): Resource = null
      override def setAvailableResources(limit: Resource) {}
      override def getCompletedContainersStatuses() = completed
      override def setCompletedContainersStatuses(containers: java.util.List[ContainerStatus]) {}
      override def setUpdatedNodes(nodes: java.util.List[NodeReport]) {}
      override def getUpdatedNodes = new java.util.ArrayList[NodeReport]()
      override def getNumClusterNodes = 1
      override def setNumClusterNodes(num: Int) {}
      override def getNMTokens = new java.util.ArrayList[NMToken]()
      override def setNMTokens(nmTokens: java.util.List[NMToken]) {}
      override def setAMCommand(command: AMCommand) {}
      override def getPreemptionMessage = null
      override def setPreemptionMessage(request: PreemptionMessage) {}
      override def getDecreasedContainers(): java.util.List[ContainerResourceDecrease] = java.util.Collections.emptyList[ContainerResourceDecrease]
      override def getIncreasedContainers(): java.util.List[ContainerResourceIncrease] = java.util.Collections.emptyList[ContainerResourceIncrease]
      override def setDecreasedContainers(decrease: java.util.List[ContainerResourceDecrease]): Unit = Unit
      override def setIncreasedContainers(increase: java.util.List[ContainerResourceIncrease]): Unit = Unit

      override def getAMCommand = if (reboot) {
        throw new ApplicationAttemptNotFoundException("Test - out of sync")
      } else {
        null
      }
      override def getAMRMToken: Token = null
      override def setAMRMToken(amRMToken: Token): Unit = {}
    }
}
