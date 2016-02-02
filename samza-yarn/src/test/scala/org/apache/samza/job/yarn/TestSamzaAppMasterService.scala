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

import java.io.BufferedReader
import java.net.URL
import java.io.InputStreamReader
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.samza.Partition
import org.apache.samza.config.MapConfig
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.{SystemStreamMetadata, SystemStreamPartition, SystemAdmin, SystemFactory}
import org.junit.Assert._
import org.junit.Test
import scala.collection.JavaConversions._
import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.container.TaskName
import org.apache.samza.coordinator.JobCoordinator
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory

class TestSamzaAppMasterService {

  @Test
  def testAppMasterDashboardShouldStart {
    val config = getDummyConfig
    val state = new SamzaAppState(JobCoordinator(config), -1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "", 1, 2)
    val service = new SamzaAppMasterService(config, state, null, null)
    val taskName = new TaskName("test")

    // start the dashboard
    service.onInit
    assertTrue(state.rpcUrl.getPort > 0)
    assertTrue(state.trackingUrl.getPort > 0)
    assertTrue(state.coordinatorUrl.getPort > 0)

    // check to see if it's running
    val url = new URL(state.rpcUrl.toString + "am")
    val is = url.openConnection().getInputStream();
    val reader = new BufferedReader(new InputStreamReader(is));
    var line: String = null;

    do {
      line = reader.readLine()
    } while (line != null)

    reader.close();
  }

  /**
   * This tests the rendering of the index.scaml file containing some Scala code. The objective
   * is to ensure that the rendered scala code builds correctly
   */
  @Test
  def testAppMasterDashboardWebServiceShouldStart {
    // Create some dummy config
    val config = getDummyConfig
    val state = new SamzaAppState(JobCoordinator(config), -1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "", 1, 2)
    val service = new SamzaAppMasterService(config, state, null, null)

    // start the dashboard
    service.onInit
    assertTrue(state.rpcUrl.getPort > 0)
    assertTrue(state.trackingUrl.getPort > 0)

    // Do a GET Request on the tracking port: This in turn will render index.scaml
    val url = state.trackingUrl
    val is = url.openConnection().getInputStream()
    val reader = new BufferedReader(new InputStreamReader(is))
    var line: String = null

    do {
      line = reader.readLine()
    } while (line != null)

    reader.close
  }

  private def getDummyConfig: Config = new MapConfig(Map[String, String](
    "job.name" -> "test",
    "yarn.container.count" -> "1",
    "systems.test-system.samza.factory" -> "org.apache.samza.job.yarn.MockSystemFactory",
    "yarn.container.memory.mb" -> "512",
    "yarn.package.path" -> "/foo",
    "task.inputs" -> "test-system.test-stream",
    "systems.test-system.samza.key.serde" -> "org.apache.samza.serializers.JsonSerde",
    "systems.test-system.samza.msg.serde" -> "org.apache.samza.serializers.JsonSerde",
    "yarn.container.retry.count" -> "1",
    "yarn.container.retry.window.ms" -> "1999999999",
    "job.coordinator.system" -> "coordinator",
    "systems.coordinator.samza.factory" -> classOf[MockCoordinatorStreamSystemFactory].getCanonicalName))
}

class MockSystemFactory extends SystemFactory {
  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = {
    throw new RuntimeException("Hmm. Not implemented.")
  }

  def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = {
    throw new RuntimeException("Hmm. Not implemented.")
  }

  def getAdmin(systemName: String, config: Config) = {
    new MockSystemAdmin(config.getContainerCount)
  }
}

/**
 * Helper class that returns metadata for each stream that contains numTasks partitions in it.
 */
class MockSystemAdmin(numTasks: Int) extends SystemAdmin {
  def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) = null
  def getSystemStreamMetadata(streamNames: java.util.Set[String]) = {
    streamNames.map(streamName => {
      var partitionMetadata = (0 until numTasks).map(partitionId => {
        new Partition(partitionId) -> new SystemStreamPartitionMetadata(null, null, null)
      }).toMap
      streamName -> new SystemStreamMetadata(streamName, partitionMetadata)
    }).toMap[String, SystemStreamMetadata]
  }

  override def createChangelogStream(topicName: String, numOfChangeLogPartitions: Int) {
    new UnsupportedOperationException("Method not implemented.")
  }

  override def validateChangelogStream(topicName: String, numOfChangeLogPartitions: Int) {
    new UnsupportedOperationException("Method not implemented.")
  }

  override def createCoordinatorStream(streamName: String) {
    new UnsupportedOperationException("Method not implemented.")
  }

  override def offsetComparator(offset1: String, offset2: String) = null
}
