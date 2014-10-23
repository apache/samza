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

package org.apache.samza.job

import org.junit.Assert._
import org.junit.Test
import scala.collection.JavaConversions._
import org.apache.samza.config.MapConfig
import org.apache.samza.config.ShellCommandConfig
import java.net.URL

class TestShellCommandBuilder {
  @Test
  def testEnvironmentVariables {
    val urlStr = "http://www.google.com"
    val config = new MapConfig(Map(ShellCommandConfig.COMMAND_SHELL_EXECUTE -> "foo"))
    val scb = new ShellCommandBuilder
    scb.setConfig(config)
    scb.setId(1)
    scb.setUrl(new URL(urlStr))
    val command = scb.buildCommand
    val environment = scb.buildEnvironment
    assertEquals("foo", command)
    assertEquals("1", environment(ShellCommandConfig.ENV_CONTAINER_ID))
    assertEquals(urlStr, environment(ShellCommandConfig.ENV_COORDINATOR_URL))
  }
}