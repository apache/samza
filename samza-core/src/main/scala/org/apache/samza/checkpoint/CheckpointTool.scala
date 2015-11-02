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
import org.apache.samza.checkpoint.CheckpointTool.TaskNameToCheckpointMap
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config.{Config, StreamConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.{Util, CommandLine, Logging}
import org.apache.samza.{Partition, SamzaException}
import scala.collection.JavaConversions._
import org.apache.samza.coordinator.JobCoordinator

import scala.collection.immutable.HashMap


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
 *   tasknames.<taskname>.systems.<system>.streams.<topic>.partitions.<partition>=<offset>
 *
 * The provided offset definitions will be grouped by <taskname> and written to
 * individual checkpoint entries for each <taskname>
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
  val SSP_PATTERN = "tasknames.%s." + StreamConfig.STREAM_PREFIX + "partitions.%d"
  val SSP_REGEX = Pattern.compile("tasknames\\.(.+)\\.systems\\.(.+)\\.streams\\.(.+)\\.partitions\\.([0-9]+)")

  type TaskNameToCheckpointMap = Map[TaskName, Map[SystemStreamPartition, String]]

  class CheckpointToolCommandLine extends CommandLine with Logging {
    val newOffsetsOpt =
      parser.accepts("new-offsets", "URI of file (e.g. file:///some/local/path.properties) " +
                                    "containing offsets to write to the job's checkpoint topic. " +
                                    "If not given, this tool prints out the current offsets.")
            .withRequiredArg
            .ofType(classOf[URI])
            .describedAs("path")

    var newOffsets: TaskNameToCheckpointMap = null

    def parseOffsets(propertiesFile: Config): TaskNameToCheckpointMap = {
      val taskNameSSPPairs = propertiesFile.entrySet.flatMap(entry => {
        val matcher = SSP_REGEX.matcher(entry.getKey)
        if (matcher.matches) {
          val taskname = new TaskName(matcher.group(1))
          val partition = new Partition(Integer.parseInt(matcher.group(4)))
          val ssp = new SystemStreamPartition(matcher.group(2), matcher.group(3), partition)
          Some(taskname -> Map(ssp -> entry.getValue))
        } else {
          warn("Warning: ignoring unrecognised property: %s = %s" format (entry.getKey, entry.getValue))
          None
        }
      }).toList

      if(taskNameSSPPairs.isEmpty) {
        return null
      }

      // Need to turn taskNameSSPPairs List[(taskname, Map[SystemStreamPartition, Offset])] to Map[TaskName, Map[SSP, Offset]]
      taskNameSSPPairs                      // List[(taskname, Map[SystemStreamPartition, Offset])]
        .groupBy(_._1)                      // Group by taskname
        .mapValues(m => m.map(_._2))        // Drop the extra taskname that we grouped on
        .mapValues(m => m.reduce( _ ++ _))  // Merge all the maps of SSPs->Offset into one for the whole taskname
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

  def apply(config: Config, offsets: TaskNameToCheckpointMap) = {
    val manager = config.getCheckpointManagerFactory match {
      case Some(className) =>
        Util.getObj[CheckpointManagerFactory](className).getCheckpointManager(config, new MetricsRegistryMap)
      case _ =>
        throw new SamzaException("This job does not use checkpointing (task.checkpoint.factory is not set).")
    }
    new CheckpointTool(config, offsets, manager)
  }

  def main(args: Array[String]) {
    val cmdline = new CheckpointToolCommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    val tool = CheckpointTool(config, cmdline.newOffsets)
    tool.run
  }
}

class CheckpointTool(config: Config, newOffsets: TaskNameToCheckpointMap, manager: CheckpointManager) extends Logging {

  def run {
    info("Using %s" format manager)

    // Find all the TaskNames that would be generated for this job config
    val coordinator = JobCoordinator(config)
    val taskNames = coordinator
      .jobModel
      .getContainers
      .values
      .flatMap(_.getTasks.keys)
      .toSet

    taskNames.foreach(manager.register)
    manager.start

    val lastCheckpoints = taskNames.map(tn => tn -> readLastCheckpoint(tn)).toMap

    lastCheckpoints.foreach(lcp => logCheckpoint(lcp._1, lcp._2, "Current checkpoint for taskname "+ lcp._1))

    if (newOffsets != null) {
      newOffsets.foreach(no => {
        logCheckpoint(no._1, no._2, "New offset to be written for taskname " + no._1)
        writeNewCheckpoint(no._1, no._2)
        info("Ok, new checkpoint has been written for taskname " + no._1)
      })
    }

    manager.stop
  }

  /** Load the most recent checkpoint state for all a specified TaskName. */
  def readLastCheckpoint(taskName:TaskName): Map[SystemStreamPartition, String] = {
    Option(manager.readLastCheckpoint(taskName))
            .getOrElse(new Checkpoint(new HashMap[SystemStreamPartition, String]()))
            .getOffsets
            .toMap
  }

  /**
   * Store a new checkpoint state for specified TaskName, overwriting any previous
   * checkpoint for that TaskName
   */
  def writeNewCheckpoint(tn: TaskName, newOffsets: Map[SystemStreamPartition, String]) {
    val checkpoint = new Checkpoint(newOffsets)
    manager.writeCheckpoint(tn, checkpoint)
  }

  def logCheckpoint(tn: TaskName, checkpoint: Map[SystemStreamPartition, String], prefix: String) {
    def logLine(tn:TaskName, ssp:SystemStreamPartition, offset:String) = (prefix + ": " + CheckpointTool.SSP_PATTERN + " = %s") format (tn.toString, ssp.getSystem, ssp.getStream, ssp.getPartition.getPartitionId, offset)

    checkpoint.keys.toList.sorted.foreach(ssp => info(logLine(tn, ssp, checkpoint.get(ssp).get)))
  }
}
