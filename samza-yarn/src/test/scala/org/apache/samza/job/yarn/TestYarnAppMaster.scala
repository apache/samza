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
import TestSamzaAppMasterTaskManager._
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.api.records.ResourceRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.api.records.AMResponse
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse

class TestYarnAppMaster {
  @Test
  def testAppMasterShouldShutdown {
    val amClient = getAmClient(getAppMasterResponse(false, List(), List()))
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
    new YarnAppMaster(List(listener), amClient).run
    assert(listener.init == 1)
    assert(listener.shutdown == 1)
  }

  @Test
  def testAppMasterShouldShutdownWithFailingListener {
    val amClient = getAmClient(getAppMasterResponse(false, List(), List()))
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
    new YarnAppMaster(List(listener1, listener2), amClient).run
    assert(listener1.shutdown == 1)
    assert(listener2.shutdown == 1)
  }

  @Test
  def testAppMasterShouldShutdownWithInterrupt {
    val amClient = getAmClient(getAppMasterResponse(false, List(), List()))
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
    val am = new YarnAppMaster(List(listener), amClient)
    val thread = new Thread {
      override def run {
        am.run
      }
    }
    thread.start
    thread.interrupt
    thread.join
    assert(listener.init == 1)
    assert(listener.shutdown == 1)
  }

  @Test
  def testAppMasterShouldForwardAllocatedAndCompleteContainers {
    val amClient = getAmClient(getAppMasterResponse(false, List(getContainer(null)), List(getContainerStatus(null, 1, null))))
    val listener = new YarnAppMasterListener {
      var allocated = 0
      var complete = 0
      override def shouldShutdown = (allocated == 1 && complete == 1)
      override def onContainerAllocated(container: Container) {
        allocated += 1
      }
      override def onContainerCompleted(containerStatus: ContainerStatus) {
        complete += 1
      }
    }
    new YarnAppMaster(List(listener), amClient).run
    assert(listener.allocated == 1)
    assert(listener.complete == 1)
  }

  @Test
  def testAppMasterShouldReboot {
    val amClient = getAmClient(getAppMasterResponse(true, List(), List()))
    val listener = new YarnAppMasterListener {
      var reboot = 0
      override def shouldShutdown = reboot == 1
      override def onReboot() {
        reboot += 1
      }
    }
    new YarnAppMaster(List(listener), amClient).run
    assert(listener.reboot == 1)
  }
}
