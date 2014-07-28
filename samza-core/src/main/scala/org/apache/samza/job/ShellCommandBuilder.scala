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

import java.util
import org.apache.samza.Partition
import org.apache.samza.config.ShellCommandConfig
import org.apache.samza.config.ShellCommandConfig.Config2ShellCommand
import org.apache.samza.config.serializers.JsonConfigSerializer
import org.apache.samza.container.TaskName
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.Util
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.ObjectMapper
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

object ShellCommandBuilder {
  /**
   * Jackson really hates Scala's classes, so we need to wrap up the SSP in a form Jackson will take
   */
  private class SSPWrapper(@BeanProperty var partition:java.lang.Integer = null,
                           @BeanProperty var Stream:java.lang.String = null,
                           @BeanProperty var System:java.lang.String = null) {
    def this() { this(null, null, null) }
    def this(ssp:SystemStreamPartition) { this(ssp.getPartition.getPartitionId, ssp.getSystemStream.getStream, ssp.getSystemStream.getSystem)}
  }

  def serializeSystemStreamPartitionSetToJSON(sspTaskNames: java.util.Map[TaskName,java.util.Set[SystemStreamPartition]]): String = {
    val map = new util.HashMap[TaskName, util.ArrayList[SSPWrapper]]()
    for((key, ssps) <- sspTaskNames) {
      val al = new util.ArrayList[SSPWrapper](ssps.size)
      for(ssp <- ssps) { al.add(new SSPWrapper(ssp)) }
      map.put(key, al)
    }

    new ObjectMapper().writeValueAsString(map)
  }

  def deserializeSystemStreamPartitionSetFromJSON(sspsAsJSON: String): Map[TaskName, Set[SystemStreamPartition]] = {
    val om = new ObjectMapper()

    val asWrapper = om.readValue(sspsAsJSON, new TypeReference[util.HashMap[String, util.ArrayList[SSPWrapper]]]() { }).asInstanceOf[util.HashMap[String, util.ArrayList[SSPWrapper]]]

    val taskName = for((key, sspsWrappers) <- asWrapper;
                       taskName = new TaskName(key);
                       ssps = sspsWrappers.map(w => new SystemStreamPartition(w.getSystem, w.getStream, new Partition(w.getPartition))).toSet
    ) yield(taskName -> ssps)

    taskName.toMap // to get an immutable map rather than mutable...
  }

  def serializeTaskNameToChangeLogPartitionMapping(mapping:Map[TaskName, Int]) = {
    val javaMap = new util.HashMap[TaskName, java.lang.Integer]()

    mapping.foreach(kv => javaMap.put(kv._1, Integer.valueOf(kv._2)))

    new ObjectMapper().writeValueAsString(javaMap)
  }

  def deserializeTaskNameToChangeLogPartitionMapping(taskNamesAsJSON: String): Map[TaskName, Int] = {
    val om = new ObjectMapper()

    val asMap = om.readValue(taskNamesAsJSON, new TypeReference[util.HashMap[String, java.lang.Integer]] {}).asInstanceOf[util.HashMap[String, java.lang.Integer]]

    asMap.map(kv => new TaskName(kv._1) -> kv._2.intValue()).toMap
  }
}
class ShellCommandBuilder extends CommandBuilder {
  def buildCommand() = config.getCommand

  def buildEnvironment(): java.util.Map[String, String] = {
    var streamsAndPartsString = ShellCommandBuilder.serializeSystemStreamPartitionSetToJSON(taskNameToSystemStreamPartitionsMapping) // Java to Scala set conversion
    var taskNameToChangeLogPartitionMappingString = ShellCommandBuilder.serializeTaskNameToChangeLogPartitionMapping(taskNameToChangeLogPartitionMapping.map(kv => kv._1 -> kv._2.toInt).toMap)
    var envConfig = JsonConfigSerializer.toJson(config)
    val isCompressed = if(config.isEnvConfigCompressed) "TRUE" else "FALSE"

    if (config.isEnvConfigCompressed) {
      /**
       * If the compressed option is enabled in config, compress the 'ENV_CONFIG' and 'ENV_SYSTEM_STREAMS'
       * properties. Note: This is a temporary workaround to reduce the size of the config and hence size
       * of the environment variable(s) exported while starting a Samza container (SAMZA-337)
       */
      streamsAndPartsString = Util.compress(streamsAndPartsString)
      taskNameToChangeLogPartitionMappingString = Util.compress(taskNameToChangeLogPartitionMappingString)
      envConfig = Util.compress(envConfig)
    }

    Map(
      ShellCommandConfig.ENV_CONTAINER_NAME -> name,
      ShellCommandConfig.ENV_SYSTEM_STREAMS -> streamsAndPartsString,
      ShellCommandConfig.ENV_TASK_NAME_TO_CHANGELOG_PARTITION_MAPPING -> taskNameToChangeLogPartitionMappingString,
      ShellCommandConfig.ENV_CONFIG -> envConfig,
      ShellCommandConfig.ENV_JAVA_OPTS -> config.getTaskOpts.getOrElse(""),
      ShellCommandConfig.ENV_COMPRESS_CONFIG -> isCompressed)

  }
}
