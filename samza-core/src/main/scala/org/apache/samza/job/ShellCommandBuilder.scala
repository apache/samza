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

import scala.collection.JavaConversions._
import org.apache.samza.config.ShellCommandConfig
import org.apache.samza.config.ShellCommandConfig.Config2ShellCommand
import org.apache.samza.config.serializers.JsonConfigSerializer
import org.apache.samza.util.Util

class ShellCommandBuilder extends CommandBuilder {
  def buildCommand() = config.getCommand

  def buildEnvironment(): java.util.Map[String, String] = {
    val streamsAndPartsString = Util.createStreamPartitionString(systemStreamPartitions.toSet) // Java to Scala set conversion
    
    Map(
      ShellCommandConfig.ENV_CONTAINER_NAME -> name,
      ShellCommandConfig.ENV_SYSTEM_STREAMS -> streamsAndPartsString,
      ShellCommandConfig.ENV_CONFIG -> JsonConfigSerializer.toJson(config),
      ShellCommandConfig.ENV_JAVA_OPTS -> config.getTaskOpts.getOrElse(""))
  }
}
