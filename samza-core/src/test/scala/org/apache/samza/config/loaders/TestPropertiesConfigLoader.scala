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

package org.apache.samza.config.loaders

import java.util.Collections

import org.apache.samza.SamzaException
import org.apache.samza.config.MapConfig
import org.junit.Assert._
import org.junit.Test

class TestPropertiesConfigLoader {
  val factory = new PropertiesConfigLoader()

  @Test
  def testCanReadPropertiesConfigFiles() {
    val properties = new MapConfig(Collections.singletonMap("path", getClass.getResource("/test.properties").getPath))

    val config = factory.getConfig(properties)
    assertEquals("bar", config.get("foo"))
  }

  @Test
  def testCanNotReadWithoutPath() {
    try {
      factory.getConfig(new MapConfig())
      fail("should have gotten a samza exception")
    } catch {
      case e: SamzaException => None // Do nothing
    }
  }
}