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
  val MOCK_SYSTEM_NAME1 = "mocksystem1"
  val MOCK_SYSTEM_NAME2 = "mocksystem2"
  val MOCK_SYSTEM_FACTORY_NAME1 = SYSTEM_FACTORY.format(MOCK_SYSTEM_NAME1)
  val MOCK_SYSTEM_FACTORY_NAME2 = SYSTEM_FACTORY.format(MOCK_SYSTEM_NAME2)
  val MOCK_SYSTEM_FACTORY_CLASSNAME1 = "some.factory.Class1"
  val MOCK_SYSTEM_FACTORY_CLASSNAME2 = "some.factory.Class2"

  def testClassName {
    val configMap = Map[String, String](
      MOCK_SYSTEM_FACTORY_NAME1 -> MOCK_SYSTEM_FACTORY_CLASSNAME1
    )
    val config = new MapConfig(configMap.asJava)

    assertEquals(MOCK_SYSTEM_FACTORY_CLASSNAME1, config.getSystemFactory(MOCK_SYSTEM_NAME1).getOrElse(""))
  }

  @Test
  def testGetEmptyClassNameAsNull {
    val configMap = Map[String, String](
      MOCK_SYSTEM_FACTORY_NAME1 -> "",
      MOCK_SYSTEM_FACTORY_NAME1 -> " "
    )
    val config = new MapConfig(configMap.asJava)

    assertEquals(config.getSystemFactory(MOCK_SYSTEM_NAME1), None)
    assertEquals(config.getSystemFactory(MOCK_SYSTEM_NAME2), None)
  }

  def testGetSystemNames {
    val configMap = Map[String, String](
      MOCK_SYSTEM_FACTORY_NAME1 -> MOCK_SYSTEM_FACTORY_CLASSNAME1,
      MOCK_SYSTEM_FACTORY_NAME2 -> MOCK_SYSTEM_FACTORY_CLASSNAME2
    )
    val config = new MapConfig(configMap.asJava)
    val systemNames = config.getSystemNames()

    assertTrue(systemNames.contains(MOCK_SYSTEM_NAME1))
    assertTrue(systemNames.contains(MOCK_SYSTEM_NAME2))
  }
}
