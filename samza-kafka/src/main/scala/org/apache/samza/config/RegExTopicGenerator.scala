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

import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ ZkUtils, ZKStringSerializer }
import org.apache.samza.config.KafkaConfig.{ Config2Kafka, REGEX_RESOLVED_STREAMS }
import org.apache.samza.SamzaException
import org.apache.samza.util.Util
import collection.JavaConversions._
import org.apache.samza.util.Logging
import scala.collection._
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.system.SystemStream
import scala.util.Sorting

/**
 * Dynamically determine the Kafka topics to use as input streams to the task via a regular expression.
 * For each topic that matches the regular expression, generate a series of config values for it and
 * add it to the task's input streams setting.
 *
 * job.config.rewriter.regex-input-rewriter.regex=.*stream
 * job.config.rewriter.regex-input-rewriter.system=kafka
 * job.config.rewriter.regex-input-rewriter.config.foo=bar
 *
 * Would result in:
 *
 * task.inputs=kafka.somestream
 * systems.kafka.streams.somestream.foo=bar
 *
 * @see samza.config.KafkaConfig.getRegexResolvedStreams
 *
 */
class RegExTopicGenerator extends ConfigRewriter with Logging {

  def rewrite(rewriterName: String, config: Config): Config = {
    val regex = config
      .getRegexResolvedStreams(rewriterName)
      .getOrElse(throw new SamzaException("No %s defined in config" format REGEX_RESOLVED_STREAMS))
    val systemName = config
      .getRegexResolvedSystem(rewriterName)
      .getOrElse(throw new SamzaException("No system defined for %s." format rewriterName))
    val topics = getTopicsFromZK(rewriterName, config)
    val existingInputStreams = config.getInputStreams
    val newInputStreams = new mutable.HashSet[SystemStream]
    val keysAndValsToAdd = new mutable.HashMap[String, String]

    // Find all the topics that match this regex
    val matchingStreams = topics
      .filter(_.matches(regex))
      .map(new SystemStream(systemName, _))
      .toSet

    for (m <- matchingStreams) {
      info("Generating new configs for matching stream %s." format m)

      if (existingInputStreams.contains(m)) {
        throw new SamzaException("Regex '%s' matches existing, statically defined input %s." format (regex, m))
      }

      newInputStreams.add(m)

      // For each topic that matched, generate all the specified configs
      config
        .getRegexResolvedInheritedConfig(rewriterName)
        .foreach(kv => keysAndValsToAdd.put("systems." + m.getSystem + ".streams." + m.getStream + "." + kv._1, kv._2))
    }
    // Build new inputs

    info("Generated config values for %d new topics" format newInputStreams.size)

    val inputStreams = TaskConfig.INPUT_STREAMS -> (existingInputStreams ++ newInputStreams)
      .map(Util.getNameFromSystemStream)
      .toArray
      .sortWith(_ < _)
      .mkString(",")

    new MapConfig((keysAndValsToAdd ++ config) += inputStreams)
  }

  def getTopicsFromZK(rewriterName: String, config: Config): Seq[String] = {
    val systemName = config
      .getRegexResolvedSystem(rewriterName)
      .getOrElse(throw new SamzaException("No system defined in config for rewriter %s." format rewriterName))
    val consumerConfig = config.getKafkaSystemConsumerConfig(systemName)
    val zkConnect = Option(consumerConfig.zkConnect)
      .getOrElse(throw new SamzaException("No zookeeper.connect for system %s defined in config." format systemName))
    val zkClient = new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer)

    try {
      ZkUtils.getAllTopics(zkClient)
    } finally {
      zkClient.close()
    }
  }
}
