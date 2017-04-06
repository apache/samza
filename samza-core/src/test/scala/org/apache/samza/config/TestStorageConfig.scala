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

package org.apache.samza.config


import org.junit.Test

import scala.collection.JavaConverters._
import org.apache.samza.config.StorageConfig._
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.assertEquals
import org.junit.Assert.fail

class TestStorageConfig {
  @Test
  def testIsChangelogSystem {
    val configMap = Map[String, String](
      FACTORY.format("store1") -> "some.factory.Class",
      CHANGELOG_STREAM.format("store1") -> "system1.stream1",
      FACTORY.format("store2") -> "some.factory.Class")
    val config = new MapConfig(configMap.asJava)
    assertFalse(config.isChangelogSystem("system3"))
    assertFalse(config.isChangelogSystem("system2"))
    assertTrue(config.isChangelogSystem("system1"))
  }

  @Test
  def testIsChangelogSystemSetting {
    val configMap = Map[String, String](
      FACTORY.format("store1") -> "some.factory.Class",
      CHANGELOG_STREAM.format("store1") -> "system1.stream1",
      CHANGELOG_SYSTEM -> "system2",
      CHANGELOG_STREAM.format("store2") -> "stream2",
      CHANGELOG_STREAM.format("store4") -> "stream4",
      FACTORY.format("store2") -> "some.factory.Class")
    val config = new MapConfig(configMap.asJava)
    assertFalse(config.isChangelogSystem("system3"))
    assertTrue(config.isChangelogSystem("system2"))
    assertTrue(config.isChangelogSystem("system1"))

    assertEquals("system1.stream1", config.getChangelogStream("store1").getOrElse(""));
    assertEquals("system2.stream2", config.getChangelogStream("store2").getOrElse(""));

    val configMapErr = Map[String, String](CHANGELOG_STREAM.format("store4")->"stream4")
    val configErr = new MapConfig(configMapErr.asJava)

    try {
      configErr.getChangelogStream("store4").getOrElse("")
      fail("store4 has no system defined. Should've failed.");
    } catch {
       case e: Exception => // do nothing, it is expected
    }
  }
}