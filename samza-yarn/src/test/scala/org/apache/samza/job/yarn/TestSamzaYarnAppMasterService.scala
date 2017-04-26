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

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL

import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.samza.clustermanager.SamzaApplicationState
import org.apache.samza.config.{Config, MapConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.coordinator.JobModelManager
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory
import org.apache.samza.metrics._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class TestSamzaYarnAppMasterService {

  @Test
  def testAppMasterDashboardShouldStart {
    val config = getDummyConfig
    val jobModelManager = JobModelManager(config)
    val samzaState = new SamzaApplicationState(jobModelManager)
    val registry = new MetricsRegistryMap()

    val state = new YarnAppState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "testHost", 1, 1);
    val service = new SamzaYarnAppMasterService(config, samzaState, state, registry, null)
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
    val jobModelManager = JobModelManager(config)
    val samzaState = new SamzaApplicationState(jobModelManager)
    val state = new YarnAppState(-1, ConverterUtils.toContainerId("container_1350670447861_0003_01_000002"), "testHost", 1, 1);
    val registry = new MetricsRegistryMap()

    val service = new SamzaYarnAppMasterService(config, samzaState, state, registry, null)

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
    "systems.coordinator.samza.factory" -> classOf[MockCoordinatorStreamSystemFactory].getCanonicalName).asJava)
}




