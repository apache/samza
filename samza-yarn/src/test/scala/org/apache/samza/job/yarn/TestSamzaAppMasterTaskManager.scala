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

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION
import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.samza.Partition
import org.apache.samza.config.Config
import org.apache.samza.config.MapConfig
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin
import org.apache.samza.util.Util
import org.junit.Assert._
import org.junit.Test

import TestSamzaAppMasterTaskManager._

object TestSamzaAppMasterTaskManager {
  def getContainer(containerId: ContainerId) = new Container {
    override def getId(): ContainerId = containerId
    override def setId(id: ContainerId) {}
    override def getNodeId(): NodeId = new NodeId {
      var host = ""
      var port = 12345
      override def getHost() = host
      override def setHost(host: String) = {
        this.host = host
      }
      override def getPort() = port
      override def setPort(port: Int) = {
        this.port = port
      }
      override def build() = ()
    }
    override def setNodeId(nodeId: NodeId) {}
    override def getNodeHttpAddress(): String = ""
    override def setNodeHttpAddress(nodeHttpAddress: String) {}
    override def getResource(): Resource = null
    override def setResource(resource: Resource) {}
    override def getPriority(): Priority = null
    override def setPriority(priority: Priority) {}
    override def getContainerToken(): Token = null
    override def setContainerToken(containerToken: Token) {}
    override def compareTo(c: Container): Int = containerId.compareTo(c.getId)
  }

  def getContainerStatus(containerId: ContainerId, exitCode: Int, diagnostic: String) = new ContainerStatus {
    override def getContainerId(): ContainerId = containerId
    override def setContainerId(containerId: ContainerId) {}
    override def getState(): ContainerState = null
    override def setState(state: ContainerState) {}
    override def getExitStatus(): Int = exitCode
    override def setExitStatus(exitStatus: Int) {}
    override def getDiagnostics() = diagnostic
    override def setDiagnostics(diagnostics: String) = {}
  }

  def getAmClient = (amClient: TestAMRMClientImpl) => new AMRMClientAsyncImpl(amClient, 1, SamzaAppMaster) {
    def getClient: TestAMRMClientImpl = amClient
  }

  class TestAMRMClientImpl(response: AllocateResponse) extends AMRMClientImpl[ContainerRequest] {
    var requests: List[ContainerRequest] = List[ContainerRequest]()

    def getRelease = release
    def resetRelease = release.clear
    override def registerApplicationMaster(appHostName: String, appHostPort: Int, appTrackingUrl: String): RegisterApplicationMasterResponse = null
    override def allocate(progressIndicator: Float): AllocateResponse = response
    override def unregisterApplicationMaster(appStatus: FinalApplicationStatus, appMessage: String, appTrackingUrl: String) = ()
    override def addContainerRequest(req: ContainerRequest) { requests ::= req }
    override def removeContainerRequest(req: ContainerRequest) {}
    override def getClusterNodeCount() = 1
    override def serviceInit(config: Configuration) {}
    override def serviceStart() {}
    override def serviceStop() {}
  }

  def getAppMasterResponse(reboot: Boolean, containers: List[Container], completed: List[ContainerStatus]) =
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
      override def getUpdatedNodes = List[NodeReport]()
      override def getNumClusterNodes = 1
      override def setNumClusterNodes(num: Int) {}
      override def getNMTokens = List[NMToken]()
      override def setNMTokens(nmTokens: java.util.List[NMToken]) {}
      override def setAMCommand(command: AMCommand) {}
      override def getPreemptionMessage = null
      override def setPreemptionMessage(request: PreemptionMessage) {}

      override def getAMCommand = if (reboot) {
        AMCommand.AM_RESYNC
      } else {
        null
      }
    }
}

class TestSamzaAppMasterTaskManager {
  import org.junit.Assert._

  val config = new MapConfig(Map[String, String](
    "yarn.container.count" -> "1",
    "systems.test-system.samza.factory" -> "org.apache.samza.job.yarn.MockSystemFactory",
    "yarn.container.memory.mb" -> "512",
    "yarn.package.path" -> "/foo",
    "task.inputs" -> "test-system.test-stream",
    "systems.test-system.samza.key.serde" -> "org.apache.samza.serializers.JsonSerde",
    "systems.test-system.samza.msg.serde" -> "org.apache.samza.serializers.JsonSerde",
    "yarn.countainer.retry.count" -> "1",
    "yarn.container.retry.window.ms" -> "1999999999"))

  @Test
  def testAppMasterShouldDefaultToOneContainerIfTaskCountIsNotSpecified {
    val state = new SamzaAppMasterState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "", 1, 2)
    val taskManager = new SamzaAppMasterTaskManager(clock, config, state, null, new YarnConfiguration)
    assert(state.taskCount == 1)
  }

  @Test
  def testAppMasterShouldStopWhenContainersFinish {
    val state = new SamzaAppMasterState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "", 1, 2)
    val taskManager = new SamzaAppMasterTaskManager(clock, config, state, null, new YarnConfiguration)

    assert(taskManager.shouldShutdown == false)
    taskManager.onContainerCompleted(getContainerStatus(state.containerId, 0, ""))
    assert(taskManager.shouldShutdown == true)
    assert(state.completedTasks == 1)
    assert(state.taskCount == 1)
    assert(state.status.equals(FinalApplicationStatus.SUCCEEDED))
  }

  @Test
  def testAppMasterShouldRequestANewContainerWhenATaskFails {
    val amClient = getAmClient(new TestAMRMClientImpl(getAppMasterResponse(false, List(), List())))
    val state = new SamzaAppMasterState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "", 1, 2)
    val taskManager = new SamzaAppMasterTaskManager(clock, config, state, amClient, new YarnConfiguration) {
      override def startContainer(packagePath: Path, container: Container, env: Map[String, String], cmds: String*) {
        // Do nothing.
      }
    }

    assert(taskManager.shouldShutdown == false)
    val container2 = ConverterUtils.toContainerId("container_1350670447861_0003_01_000002")
    taskManager.onInit
    taskManager.onContainerAllocated(getContainer(container2))
    taskManager.onContainerCompleted(getContainerStatus(container2, 1, "expecting a failure here"))
    assert(taskManager.shouldShutdown == false)
    // 2. First is from onInit, second is from onContainerCompleted, since it failed.
    assertEquals(2, amClient.getClient.requests.size)
    assertEquals(0, amClient.getClient.getRelease.size)
    assertFalse(taskManager.shouldShutdown)
    // Now trigger an AM shutdown since our retry count is 1, and we're failing twice
    taskManager.onContainerAllocated(getContainer(container2))
    taskManager.onContainerCompleted(getContainerStatus(container2, 1, "expecting a failure here"))
    assertEquals(2, amClient.getClient.requests.size)
    assertEquals(0, amClient.getClient.getRelease.size)
    assertTrue(taskManager.shouldShutdown)
  }

  @Test
  def testAppMasterShouldRequestANewContainerWhenATaskIsReleased {
    val amClient = getAmClient(new TestAMRMClientImpl(getAppMasterResponse(false, List(), List())))
    val state = new SamzaAppMasterState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "", 1, 2)
    state.taskCount = 2
    var containersRequested = 0
    var containersStarted = 0
    val taskManager = new SamzaAppMasterTaskManager(clock, config, state, amClient, new YarnConfiguration) {
      override def startContainer(packagePath: Path, container: Container, env: Map[String, String], cmds: String*) {
        containersStarted += 1
      }

      override def requestContainers(memMb: Int, cpuCores: Int, containers: Int) {
        containersRequested += 1
        super.requestContainers(memMb, cpuCores, containers)
      }
    }
    val container2 = ConverterUtils.toContainerId("container_1350670447861_0003_01_000002")
    val container3 = ConverterUtils.toContainerId("container_1350670447861_0003_01_000003")

    assert(taskManager.shouldShutdown == false)
    taskManager.onInit
    assert(taskManager.shouldShutdown == false)
    assert(amClient.getClient.requests.size == 1)
    assert(amClient.getClient.getRelease.size == 0)

    // allocate container 2
    taskManager.onContainerAllocated(getContainer(container2))
    assert(state.neededContainers == 0)
    assert(state.runningTasks.size == 1)
    assert(state.taskPartitions.size == 1)
    assert(state.unclaimedTasks.size == 0)
    assert(containersRequested == 1)
    assert(containersStarted == 1)

    // allocate an extra container, which the AM doesn't need, and should be released
    taskManager.onContainerAllocated(getContainer(container3))
    assert(state.neededContainers == 0)
    assert(state.runningTasks.size == 1)
    assert(state.taskPartitions.size == 1)
    assert(state.unclaimedTasks.size == 0)
    assert(amClient.getClient.requests.size == 1)
    assert(amClient.getClient.getRelease.size == 1)
    assert(amClient.getClient.getRelease.head.equals(container3))

    // reset the helper state, so we can make sure that releasing the container (next step) doesn't request more resources
    amClient.getClient.requests = List()
    amClient.getClient.resetRelease

    // now release the container, and make sure the AM doesn't ask for more
    assert(taskManager.shouldShutdown == false)
    taskManager.onContainerCompleted(getContainerStatus(container3, -100, "pretend the container was released"))
    assert(taskManager.shouldShutdown == false)
    assert(state.neededContainers == 0)
    assert(state.runningTasks.size == 1)
    assert(state.taskPartitions.size == 1)
    assert(state.unclaimedTasks.size == 0)
    assert(amClient.getClient.requests.size == 0)
    assert(amClient.getClient.getRelease.size == 0)

    // pretend container 2 is released due to an NM failure, and make sure that the AM requests a new container
    assert(taskManager.shouldShutdown == false)
    taskManager.onContainerCompleted(getContainerStatus(container2, -100, "pretend the container was 'lost' due to an NM failure"))
    assert(taskManager.shouldShutdown == false)
    assert(amClient.getClient.requests.size == 1)
    assert(amClient.getClient.getRelease.size == 0)
  }

  @Test
  def testAppMasterShouldWorkWithMoreThanOneContainer {
    val map = new java.util.HashMap[String, String](config)
    map.put("yarn.container.count", "2")
    val newConfig = new MapConfig(map)
    val amClient = getAmClient(new TestAMRMClientImpl(getAppMasterResponse(false, List(), List())))
    val state = new SamzaAppMasterState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "", 1, 2)
    state.taskCount = 2
    var containersStarted = 0
    val taskManager = new SamzaAppMasterTaskManager(clock, newConfig, state, amClient, new YarnConfiguration) {
      override def startContainer(packagePath: Path, container: Container, env: Map[String, String], cmds: String*) {
        containersStarted += 1
      }
    }
    val container2 = ConverterUtils.toContainerId("container_1350670447861_0003_01_000002")
    val container3 = ConverterUtils.toContainerId("container_1350670447861_0003_01_000003")

    assert(taskManager.shouldShutdown == false)
    taskManager.onInit
    assert(taskManager.shouldShutdown == false)
    assert(amClient.getClient.requests.size == 2)
    assert(amClient.getClient.getRelease.size == 0)
    taskManager.onContainerAllocated(getContainer(container2))
    assert(state.neededContainers == 1)
    assert(state.runningTasks.size == 1)
    assert(state.taskPartitions.size == 1)
    assert(state.unclaimedTasks.size == 1)
    assert(containersStarted == 1)
    taskManager.onContainerAllocated(getContainer(container3))
    assert(state.neededContainers == 0)
    assert(state.runningTasks.size == 2)
    assert(state.taskPartitions.size == 2)
    assert(state.unclaimedTasks.size == 0)
    assert(containersStarted == 2)

    // container2 finishes successfully
    taskManager.onContainerCompleted(getContainerStatus(container2, 0, ""))
    assert(state.neededContainers == 0)
    assert(state.runningTasks.size == 1)
    assert(state.taskPartitions.size == 1)
    assert(state.unclaimedTasks.size == 0)
    assert(state.completedTasks == 1)

    // container3 fails
    taskManager.onContainerCompleted(getContainerStatus(container3, 1, "expected failure here"))
    assert(state.neededContainers == 1)
    assert(state.runningTasks.size == 0)
    assert(state.taskPartitions.size == 0)
    assert(state.unclaimedTasks.size == 1)
    assert(state.completedTasks == 1)
    assert(taskManager.shouldShutdown == false)

    // container3 is re-allocated
    taskManager.onContainerAllocated(getContainer(container3))
    assert(state.neededContainers == 0)
    assert(state.runningTasks.size == 1)
    assert(state.taskPartitions.size == 1)
    assert(state.unclaimedTasks.size == 0)
    assert(containersStarted == 3)

    // container3 finishes sucecssfully
    taskManager.onContainerCompleted(getContainerStatus(container3, 0, ""))
    assert(state.neededContainers == 0)
    assert(state.runningTasks.size == 0)
    assert(state.taskPartitions.size == 0)
    assert(state.unclaimedTasks.size == 0)
    assert(state.completedTasks == 2)
    assert(taskManager.shouldShutdown == true)
  }

  @Test
  def testAppMasterShouldReleaseExtraContainers {
    val amClient = getAmClient(new TestAMRMClientImpl(getAppMasterResponse(false, List(), List())))
    val state = new SamzaAppMasterState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000001"), "", 1, 2)
    var containersRequested = 0
    var containersStarted = 0
    val taskManager = new SamzaAppMasterTaskManager(clock, config, state, amClient, new YarnConfiguration) {
      override def startContainer(packagePath: Path, container: Container, env: Map[String, String], cmds: String*) {
        containersStarted += 1
      }

      override def requestContainers(memMb: Int, cpuCores: Int, containers: Int) {
        containersRequested += 1
        super.requestContainers(memMb, cpuCores, containers)
      }
    }
    val container2 = ConverterUtils.toContainerId("container_1350670447861_0003_01_000002")
    val container3 = ConverterUtils.toContainerId("container_1350670447861_0003_01_000003")

    assert(taskManager.shouldShutdown == false)
    taskManager.onInit
    assert(taskManager.shouldShutdown == false)
    assert(amClient.getClient.requests.size == 1)
    assert(amClient.getClient.getRelease.size == 0)
    assert(state.neededContainers == 1)
    assert(state.runningTasks.size == 0)
    assert(state.taskPartitions.size == 0)
    assert(state.unclaimedTasks.size == 1)
    taskManager.onContainerAllocated(getContainer(container2))
    assert(state.neededContainers == 0)
    assert(state.runningTasks.size == 1)
    assert(state.taskPartitions.size == 1)
    assert(state.unclaimedTasks.size == 0)
    assert(containersRequested == 1)
    assert(containersStarted == 1)
    taskManager.onContainerAllocated(getContainer(container3))
    assert(state.neededContainers == 0)
    assert(state.runningTasks.size == 1)
    assert(state.taskPartitions.size == 1)
    assert(state.unclaimedTasks.size == 0)
    assert(containersRequested == 1)
    assert(containersStarted == 1)
    assert(amClient.getClient.requests.size == 1)
    assert(amClient.getClient.getRelease.size == 1)
    assert(amClient.getClient.getRelease.head.equals(container3))
  }

  @Test
  def testPartitionsShouldWorkWithMoreTasksThanPartitions {
    val onePartition = Set(new SystemStreamPartition("system", "stream", new Partition(0)))
    assertEquals(Util.getStreamsAndPartitionsForContainer(0, 2, onePartition), Set(new SystemStreamPartition("system", "stream", new Partition(0))))
    assertEquals(Util.getStreamsAndPartitionsForContainer(1, 2, onePartition), Set())
  }

  @Test
  def testPartitionsShouldWorkWithMorePartitionsThanTasks {
    val fivePartitions = (0 until 5).map(p => new SystemStreamPartition("system", "stream", new Partition(p))).toSet
    assertEquals(Util.getStreamsAndPartitionsForContainer(0, 2, fivePartitions), Set(new SystemStreamPartition("system", "stream", new Partition(0)), new SystemStreamPartition("system", "stream", new Partition(2)), new SystemStreamPartition("system", "stream", new Partition(4))))
    assertEquals(Util.getStreamsAndPartitionsForContainer(1, 2, fivePartitions), Set(new SystemStreamPartition("system", "stream", new Partition(1)), new SystemStreamPartition("system", "stream", new Partition(3))))
  }

  @Test
  def testPartitionsShouldWorkWithTwelvePartitionsAndFiveContainers {
    val fivePartitions = (0 until 12).map(p => new SystemStreamPartition("system", "stream", new Partition(p))).toSet
    assertEquals(Util.getStreamsAndPartitionsForContainer(0, 5, fivePartitions), Set(new SystemStreamPartition("system", "stream", new Partition(0)), new SystemStreamPartition("system", "stream", new Partition(5)), new SystemStreamPartition("system", "stream", new Partition(10))))
    assertEquals(Util.getStreamsAndPartitionsForContainer(1, 5, fivePartitions), Set(new SystemStreamPartition("system", "stream", new Partition(1)), new SystemStreamPartition("system", "stream", new Partition(6)), new SystemStreamPartition("system", "stream", new Partition(11))))
    assertEquals(Util.getStreamsAndPartitionsForContainer(2, 5, fivePartitions), Set(new SystemStreamPartition("system", "stream", new Partition(2)), new SystemStreamPartition("system", "stream", new Partition(7))))
    assertEquals(Util.getStreamsAndPartitionsForContainer(3, 5, fivePartitions), Set(new SystemStreamPartition("system", "stream", new Partition(3)), new SystemStreamPartition("system", "stream", new Partition(8))))
    assertEquals(Util.getStreamsAndPartitionsForContainer(4, 5, fivePartitions), Set(new SystemStreamPartition("system", "stream", new Partition(4)), new SystemStreamPartition("system", "stream", new Partition(9))))
  }

  @Test
  def testPartitionsShouldWorkWithEqualPartitionsAndTasks {
    val twoPartitions = (0 until 2).map(p => new SystemStreamPartition("system", "stream", new Partition(p))).toSet
    assertEquals(Util.getStreamsAndPartitionsForContainer(0, 2, twoPartitions), Set(new SystemStreamPartition("system", "stream", new Partition(0))))
    assertEquals(Util.getStreamsAndPartitionsForContainer(1, 2, twoPartitions), Set(new SystemStreamPartition("system", "stream", new Partition(1))))
    assertEquals(Util.getStreamsAndPartitionsForContainer(0, 1, Set(new SystemStreamPartition("system", "stream", new Partition(0)))), Set(new SystemStreamPartition("system", "stream", new Partition(0))))
  }

  val clock = () => System.currentTimeMillis
}

class MockSystemFactory extends SystemFactory {
  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = {
    throw new RuntimeException("Hmm. Not implemented.")
  }

  def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = {
    throw new RuntimeException("Hmm. Not implemented.")
  }

  def getAdmin(systemName: String, config: Config) = {
    new SinglePartitionWithoutOffsetsSystemAdmin
  }
}
