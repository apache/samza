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

package org.apache.samza.checkpoint

import java.net.URI
import java.util.regex.Pattern
import joptsimple.OptionSet
import org.apache.samza.{Partition, SamzaException}
import org.apache.samza.config.{Config, StreamConfig}
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.{CommandLine, Util}
import scala.collection.JavaConversions._
import grizzled.slf4j.Logging

/**
 * Command-line tool for inspecting and manipulating the checkpoints for a job.
 * This can be used, for example, to force a job to re-process a stream from the
 * beginning.
 *
 * When running this tool, you need to provide the configuration URI of job you
 * want to inspect/manipulate. The tool prints out the latest checkpoint for that
 * job (latest offset of each partition of each input stream).
 *
 * To update the checkpoint, you need to provide a second properties file
 * containing the offsets you want. It needs to be in the same format as the tool
 * prints out the latest checkpoint:
 *
 *   systems.<system>.streams.<topic>.partitions.<partition>=<offset>
 *
 * NOTE: A job only reads its checkpoint when it starts up. Therefore, if you want
 * your checkpoint change to take effect, you have to first stop the job, then
 * write a new checkpoint, then start it up again. Writing a new checkpoint while
 * the job is running may not have any effect.
 *
 * If you're building Samza from source, you can use the 'checkpointTool' gradle
 * task as a short-cut to running this tool.
 */
object CheckpointTool {
  /** Format in which SystemStreamPartition is represented in a properties file */
  val SSP_PATTERN = StreamConfig.STREAM_PREFIX + "partitions.%d"
  val SSP_REGEX = Pattern.compile("systems\\.(.+)\\.streams\\.(.+)\\.partitions\\.([0-9]+)")

  class CheckpointToolCommandLine extends CommandLine with Logging {
    val newOffsetsOpt =
      parser.accepts("new-offsets", "URI of file (e.g. file:///some/local/path.properties) " +
                                    "containing offsets to write to the job's checkpoint topic. " +
                                    "If not given, this tool prints out the current offsets.")
            .withRequiredArg
            .ofType(classOf[URI])
            .describedAs("path")

    var newOffsets: Map[SystemStreamPartition, String] = null

    def parseOffsets(propertiesFile: Config): Map[SystemStreamPartition, String] = {
      propertiesFile.entrySet.flatMap(entry => {
        val matcher = SSP_REGEX.matcher(entry.getKey)
        if (matcher.matches) {
          val partition = new Partition(Integer.parseInt(matcher.group(3)))
          val ssp = new SystemStreamPartition(matcher.group(1), matcher.group(2), partition)
          Some(ssp -> entry.getValue)
        } else {
          warn("Warning: ignoring unrecognised property: %s = %s" format (entry.getKey, entry.getValue))
          None
        }
      }).toMap
    }

    override def loadConfig(options: OptionSet) = {
      val config = super.loadConfig(options)
      if (options.has(newOffsetsOpt)) {
        val properties = configFactory.getConfig(options.valueOf(newOffsetsOpt))
        newOffsets = parseOffsets(properties)
      }
      config
    }
  }

  def main(args: Array[String]) {
    val cmdline = new CheckpointToolCommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    val tool = new CheckpointTool(config, cmdline.newOffsets)
    tool.run
  }
}

class CheckpointTool(config: Config, newOffsets: Map[SystemStreamPartition, String]) extends Logging {
  val manager = config.getCheckpointManagerFactory match {
    case Some(className) =>
      Util.getObj[CheckpointManagerFactory](className).getCheckpointManager(config, new MetricsRegistryMap)
    case _ =>
      throw new SamzaException("This job does not use checkpointing (task.checkpoint.factory is not set).")
  }

  // The CheckpointManagerFactory needs to perform this same operation when initializing
  // the manager. TODO figure out some way of avoiding duplicated work.
  val partitions = Util.getInputStreamPartitions(config).map(_.getPartition).toSet

  def run {
    info("Using %s" format manager)
    partitions.foreach(manager.register)
    manager.start
    val lastCheckpoint = readLastCheckpoint

    logCheckpoint(lastCheckpoint, "Current checkpoint")

    if (newOffsets != null) {
      logCheckpoint(newOffsets, "New offset to be written")
      writeNewCheckpoint(newOffsets)
      manager.stop
      info("Ok, new checkpoint has been written.")
    }
  }

  /** Load the most recent checkpoint state for all partitions. */
  def readLastCheckpoint: Map[SystemStreamPartition, String] = {
    partitions.flatMap(partition => {
      manager.readLastCheckpoint(partition)
        .getOffsets
        .map { case (systemStream, offset) =>
          new SystemStreamPartition(systemStream, partition) -> offset
        }
    }).toMap
  }

  /**
   * Store a new checkpoint state for all given partitions, overwriting the
   * current state. Any partitions that are not mentioned will not
   * be changed.
   */
  def writeNewCheckpoint(newOffsets: Map[SystemStreamPartition, String]) {
    newOffsets.groupBy(_._1.getPartition).foreach {
      case (partition, offsets) =>
        val streamOffsets = offsets.map { case (ssp, offset) => ssp.getSystemStream -> offset }.toMap
        val checkpoint = new Checkpoint(streamOffsets)
        manager.writeCheckpoint(partition, checkpoint)
    }
  }

  def logCheckpoint(checkpoint: Map[SystemStreamPartition, String], prefix: String) {
    checkpoint.map { case (ssp, offset) =>
      (CheckpointTool.SSP_PATTERN + " = %s") format (ssp.getSystem, ssp.getStream, ssp.getPartition.getPartitionId, offset)
    }.toList.sorted.foreach(line => info(prefix + ": " + line))
  }
}
