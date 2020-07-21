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

package org.apache.samza.job.local

import com.google.common.collect.ImmutableMap
import org.apache.samza.config.MapConfig
import org.apache.samza.coordinator.{JobModelManager, MetadataResourceUtil}
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory
import org.apache.samza.job.ApplicationStatus.{Running, SuccessfulFinish, UnsuccessfulFinish}
import org.apache.samza.job.CommandBuilder
import org.apache.samza.system.{SystemAdmin, SystemConsumer, SystemProducer}
import org.apache.samza.util.NoOpMetricsRegistry
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

object TestProcessJob {

  val OneSecondCommand = "sleep 1"
  val TenSecondCommand = "sleep 10"
  val SimpleCommand = "true"
  val FailingCommand = "false"
  val BadCommand = "bad-non-existing-command"
  val MockSystemName = "test-kafka"
  val MockConfigs = new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", MockSystemName))

  private def createProcessJob(command: String): ProcessJob = {
    val commandBuilder = new CommandBuilder {
      override def buildCommand = command

      override def buildEnvironment = Map[String, String]().asJava
    }
    // Setup for mocking a CoordinateStreamStore
    val systemFactory = new MockCoordinatorStreamSystemFactory
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache
    val systemConsumer = systemFactory.getConsumer(MockSystemName, MockConfigs, new NoOpMetricsRegistry)
    val systemProducer = systemFactory.getProducer(MockSystemName, MockConfigs, new NoOpMetricsRegistry)
    val systemAdmin = systemFactory.getAdmin(MockSystemName, MockConfigs)

    new ProcessJob(
      commandBuilder,
      new MockJobModelManager,
      new MockCoordinateStreamStore(MockConfigs, systemProducer, systemConsumer, systemAdmin),
      new MockMetadataResourceUtil)
  }

  private def getMockJobModelManager(processJob: ProcessJob): MockJobModelManager = {
    processJob.jobModelManager.asInstanceOf[MockJobModelManager]
  }

  private def getMockCoordinatorStreamStore(processJob: ProcessJob): MockCoordinateStreamStore = {
    processJob.coordinatorStreamStore.asInstanceOf[MockCoordinateStreamStore]
  }

  private def getMockMetadataResourceUtil(processJob: ProcessJob): MockMetadataResourceUtil = {
    processJob.metadataResourceUtil.asInstanceOf[MockMetadataResourceUtil]
  }
}

class TestProcessJob {

  import TestProcessJob._

  @Test
  def testProcessJobShouldFinishOnItsOwn: Unit = {
    val processJob = createProcessJob(SimpleCommand)

    val status = processJob.submit.waitForFinish(0)

    assertEquals(SuccessfulFinish, status)
    assertTrue(getMockJobModelManager(processJob).stopped)
    assertTrue(getMockCoordinatorStreamStore(processJob).closed)
    assertTrue(getMockMetadataResourceUtil(processJob).stopped)
  }

  @Test
  def testProcessJobShouldReportFailingCommands: Unit = {
    val processJob = createProcessJob(FailingCommand)

    val status = processJob.submit.waitForFinish(0)

    assertEquals(UnsuccessfulFinish, status)
    assertTrue(getMockJobModelManager(processJob).stopped)
    assertTrue(getMockCoordinatorStreamStore(processJob).closed)
    assertTrue(getMockMetadataResourceUtil(processJob).stopped)
  }

  @Test
  def testProcessJobWaitForFinishShouldTimeOut: Unit = {
    val processJob = createProcessJob(OneSecondCommand)

    // Wait for a shorter duration than that necessary for the specified command to complete.
    val status = processJob.submit.waitForFinish(10)

    assertEquals(Running, status)
  }

  @Test
  def testProcessJobKillShouldWork: Unit = {
    val processJob = createProcessJob(TenSecondCommand)

    processJob.submit.kill

    assertEquals(UnsuccessfulFinish, processJob.getStatus)
    assertTrue(getMockJobModelManager(processJob).stopped)
    assertTrue(getMockCoordinatorStreamStore(processJob).closed)
    assertTrue(getMockMetadataResourceUtil(processJob).stopped)
  }

  @Test
  def testProcessJobSubmitBadProcessShouldFailGracefully: Unit = {
    val processJob = createProcessJob(BadCommand)

    processJob.submit.waitForFinish(0)

    assertEquals(UnsuccessfulFinish, processJob.getStatus)
    assertTrue(getMockJobModelManager(processJob).stopped)
    assertTrue(getMockCoordinatorStreamStore(processJob).closed)
    assertTrue(getMockMetadataResourceUtil(processJob).stopped)
  }

  @Test
  def testProcessJobWaitForStatusShouldWork: Unit = {
    val processJob = createProcessJob(SimpleCommand)

    processJob.submit.waitForStatus(SuccessfulFinish, 0)

    assertEquals(SuccessfulFinish, processJob.getStatus)
    assertTrue(getMockJobModelManager(processJob).stopped)
    assertTrue(getMockCoordinatorStreamStore(processJob).closed)
    assertTrue(getMockMetadataResourceUtil(processJob).stopped)
  }

  @Test
  def testProcessJobWaitForStatusShouldTimeOut: Unit = {
    val processJob = createProcessJob(OneSecondCommand)

    // Wait for a shorter duration than that necessary for the specified command to complete.
    val status = processJob.submit.waitForStatus(SuccessfulFinish, 10)

    assertEquals(Running, status)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testProcessJobWaitForStatusShouldThrowOnNegativeTimeout: Unit = {
    val processJob = createProcessJob(SimpleCommand)
    processJob.waitForStatus(SuccessfulFinish, -1)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testProcessJobWaitForFinishShouldThrowOnNegativeTimeout: Unit = {
    val processJob = createProcessJob(SimpleCommand)
    processJob.waitForFinish(-1)
  }
}

class MockJobModelManager extends JobModelManager(null, null) {
  var stopped: Boolean = false

  override def start: Unit = {}

  override def stop: Unit = {
    stopped = true
  }
}

class MockCoordinateStreamStore(
  config: MapConfig,
  systemProducer: SystemProducer,
  systemConsumer: SystemConsumer,
  systemAdmin: SystemAdmin)
  extends CoordinatorStreamStore(config, systemProducer, systemConsumer, systemAdmin) {
  var closed: Boolean = false

  override def init: Unit = {}

  override def close: Unit = {
    closed = true
  }
}

class MockMetadataResourceUtil extends MetadataResourceUtil(null, null, null) {
  var stopped: Boolean = false

  override def createResources: Unit = {}

  override def stop: Unit = {
    stopped = true
  }
}
