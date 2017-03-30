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


import java.io.File

import org.apache.samza.config.ShellCommandConfig
import org.apache.samza.config.ShellCommandConfig.Config2ShellCommand
import scala.collection.JavaConverters._

class ShellCommandBuilder extends CommandBuilder {
  def buildCommand() = {
    if(commandPath == null || commandPath.isEmpty())
      config.getCommand
    else
      commandPath + File.separator +  config.getCommand
  }

  def buildEnvironment(): java.util.Map[String, String] = {
    val envMap = Map(
      ShellCommandConfig.ENV_CONTAINER_ID -> id.toString,
      ShellCommandConfig.ENV_COORDINATOR_URL -> url.toString,
      ShellCommandConfig.ENV_JAVA_OPTS -> config.getTaskOpts.getOrElse(""))

    val envMapWithJavaHome = config.getJavaHome match {
      case Some(javaHome) => envMap + (ShellCommandConfig.ENV_JAVA_HOME -> javaHome)
      case None => envMap
    }

    envMapWithJavaHome.asJava
  }
}
