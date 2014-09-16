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

import TestSamzaAppMasterTaskManager._

import org.apache.hadoop.yarn.api.records.{ Container, ContainerStatus }
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.junit.Test
import org.junit.Assert._

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION

class TestSamzaAppMaster {
  @Test
  def testAppMasterShouldShutdown {
    val amClient = getAmClient(new TestAMRMClientImpl(getAppMasterResponse(false, List(), List())))
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
    val amClient = getAmClient(new TestAMRMClientImpl(getAppMasterResponse(false, List(), List())))
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
    val amClient = getAmClient(new TestAMRMClientImpl(getAppMasterResponse(false, List(), List())))
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
    val amClient = getAmClient(new TestAMRMClientImpl(getAppMasterResponse(false, List(getContainer(null)), List(getContainerStatus(null, 1, null)))))
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
    val amClient = getAmClient(new TestAMRMClientImpl(getAppMasterResponse(true, List(), List())))
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
}
