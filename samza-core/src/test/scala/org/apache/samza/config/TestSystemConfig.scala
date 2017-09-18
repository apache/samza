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

import scala.collection.JavaConverters._
import org.apache.samza.config.SystemConfig.{Config2System, SYSTEM_FACTORY}
import org.junit.Assert._
import org.junit.Test

class TestSystemConfig {
  @Test
  def testClassName {
    val mockClassName = "some.factory.Class"
    val configMap = Map[String, String](
      SYSTEM_FACTORY.format("mocksystem") -> mockClassName
    )
    val config = new MapConfig(configMap.asJava)
    assertEquals(mockClassName, config.getSystemFactory("mocksystem").getOrElse(""))
  }

  @Test
  def testGetEmptyClassNameAsNull {
    val configMap = Map[String, String](
      SYSTEM_FACTORY.format("mocksystem") -> ""
    )
    val config = new MapConfig(configMap.asJava)
    assertEquals(config.getSystemFactory("mocksystem"), None)
  }
}
