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
package org.apache.samza.util

import java.util
import java.util.Collections

import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde
import org.apache.samza.coordinator.stream.messages.SetConfig
import org.apache.samza.system.{StreamSpec, SystemAdmin, SystemStream}
import org.junit.{Assert, Test}
import org.mockito.Matchers.any
import org.mockito.Mockito
import org.apache.samza.config.{JobConfig, MapConfig}
import org.apache.samza.metadatastore.MetadataStore

class TestCoordinatorStreamUtil {

  @Test
  def testCreateCoordinatorStream() {
    val systemStream = Mockito.spy(new SystemStream("testSystem", "testStream"))
    val systemAdmin = Mockito.mock(classOf[SystemAdmin])

    CoordinatorStreamUtil.createCoordinatorStream(systemStream, systemAdmin)
    Mockito.verify(systemStream).getStream
    Mockito.verify(systemAdmin).createStream(any(classOf[StreamSpec]))
  }

  @Test
  def testBuildCoordinatorStreamConfig() {
    val addConfig = new util.HashMap[String, String]
    addConfig.put("job.name", "test-job-name")
    addConfig.put("job.id", "i001")
    addConfig.put("job.coordinator.system", "samzatest")
    addConfig.put("systems.samzatest.test","test")
    addConfig.put("test.only","nothing")
    val config = new MapConfig(addConfig)
    val configMap = CoordinatorStreamUtil.buildCoordinatorStreamConfig(config)

    Assert.assertEquals(configMap.get("systems.samzatest.test"), "test")
    Assert.assertEquals(configMap.get("test.only"), null)
  }

  @Test
  def testReadConfigFromCoordinatorStream() {
    val keyForNonBlankVal = "app.id"
    val nonBlankVal = "1"
    val keyForEmptyVal = "task.opt"
    val emptyVal = ""
    val keyForNullVal = "zk.server"
    val nullVal = null

    val valueSerde = new CoordinatorStreamValueSerde(SetConfig.TYPE)
    val configMap = new util.HashMap[String, Array[Byte]]() {
      put(CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(SetConfig.TYPE, keyForNonBlankVal),
        valueSerde.toBytes(nonBlankVal))
      put(CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(SetConfig.TYPE, keyForEmptyVal),
        valueSerde.toBytes(emptyVal))
      put(CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(SetConfig.TYPE, keyForNullVal),
        valueSerde.toBytes(nullVal))
    }

    val coordinatorStreamStore = Mockito.mock(classOf[CoordinatorStreamStore])
    Mockito.when(coordinatorStreamStore.all()).thenReturn(configMap)

    val configFromCoordinatorStream = CoordinatorStreamUtil.readConfigFromCoordinatorStream(coordinatorStreamStore)

    Assert.assertEquals(configFromCoordinatorStream.get(keyForNonBlankVal), nonBlankVal)
    Assert.assertEquals(configFromCoordinatorStream.get(keyForEmptyVal), emptyVal)
    Assert.assertFalse(configFromCoordinatorStream.containsKey(keyForNullVal))
  }

  @Test
  def testWriteConfigToCoordinatorStream() {
    val addConfig = new util.HashMap[String, String]
    addConfig.put("job.name", "test-job-name")
    addConfig.put("job.id", "i001")
    addConfig.put("job.coordinator.system", "samzatest")
    addConfig.put("systems.samzatest.test","test")
    addConfig.put("test.only","nothing")
    addConfig.put("systems.samzatest.samza.factory", "org.apache.samza.system.MockSystemFactory")
    val config = new MapConfig(addConfig)
    val configMap = CoordinatorStreamUtil.buildCoordinatorStreamConfig(config)

    CoordinatorStreamUtil.writeConfigToCoordinatorStream(configMap)
  }

  @Test
  def testReadLaunchConfigFromCoordinatorStream() {
    // Empty config when auto sizing is disabled.
    Assert.assertEquals(new MapConfig(),  CoordinatorStreamUtil.readLaunchConfigFromCoordinatorStream(new MapConfig(), null))

    val valueSerde = new CoordinatorStreamValueSerde(SetConfig.TYPE)
    val config = new MapConfig(Collections.singletonMap(JobConfig.JOB_AUTOSIZING_ENABLED, "true"))
    val expected = new MapConfig(Collections.singletonMap(JobConfig.JOB_AUTOSIZING_CONTAINER_COUNT, "20"))
    val mockMetadataStore = Mockito.mock(classOf[MetadataStore])
    val configMap = new util.HashMap[String, Array[Byte]]() {
      put(CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(SetConfig.TYPE,
        JobConfig.JOB_ID),
        valueSerde.toBytes("321"))
      put(CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(SetConfig.TYPE,
        JobConfig.JOB_AUTOSIZING_CONTAINER_COUNT),
        valueSerde.toBytes("20"))
    }
    Mockito.when(mockMetadataStore.all()).thenReturn(configMap)

    // Verify the launch config is expected
    Assert.assertEquals(expected, CoordinatorStreamUtil.readLaunchConfigFromCoordinatorStream(config, mockMetadataStore))
  }
}
