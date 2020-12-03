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

import java.io.FileInputStream
import java.util
import java.util.Properties
import java.util.regex.Pattern

import joptsimple.ArgumentAcceptingOptionSpec
import joptsimple.OptionSet
import org.apache.samza.checkpoint.CheckpointTool.TaskNameToCheckpointMap
import org.apache.samza.config._
import org.apache.samza.container.TaskName
import org.apache.samza.job.JobRunner.info
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.{CommandLine, ConfigUtil, CoordinatorStreamUtil, Logging}
import org.apache.samza.Partition
import org.apache.samza.SamzaException

import scala.collection.JavaConverters._
import org.apache.samza.coordinator.JobModelManager
import org.apache.samza.coordinator.metadatastore.{CoordinatorStreamStore, NamespaceAwareCoordinatorStreamStore}
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping
import org.apache.samza.execution.JobPlanner
import org.apache.samza.storage.ChangelogStreamManager
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals

import scala.collection.mutable.ListBuffer


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
  val SSP_PATTERN = "tasknames.%s.systems.%s.streams.%s.partitions.%d"
  val SSP_REGEX: Pattern = Pattern.compile("tasknames\\.(.+)\\.systems\\.(.+)\\.streams\\.(.+)\\.partitions\\.([0-9]+)")

  type TaskNameToCheckpointMap = Map[TaskName, Map[SystemStreamPartition, String]]

  class CheckpointToolCommandLine extends CommandLine with Logging {
    val newOffsetsOpt: ArgumentAcceptingOptionSpec[String] =
      parser.accepts("new-offsets", "Location of file (e.g. /some/local/path.properties) " +
                                    "containing offsets to write to the job's checkpoint topic. " +
                                    "If not given, this tool prints out the current offsets.")
            .withRequiredArg
            .ofType(classOf[String])
            .describedAs("path")

    var newOffsets: TaskNameToCheckpointMap = _

    def parseOffsets(propertiesFile: Properties): TaskNameToCheckpointMap = {
      var checkpoints : ListBuffer[(TaskName, Map[SystemStreamPartition, String])] = ListBuffer()
      propertiesFile.asScala.foreach { case (key, value) =>
        val matcher = SSP_REGEX.matcher(key)
        if (matcher.matches) {
          val taskname = new TaskName(matcher.group(1))
          val partition = new Partition(Integer.parseInt(matcher.group(4)))
          val ssp = new SystemStreamPartition(matcher.group(2), matcher.group(3), partition)
          val tuple = taskname -> Map(ssp -> value)
          checkpoints += tuple
        } else {
          warn("Warning: ignoring unrecognised property: %s = %s" format (key, value))
        }
      }
      val taskNameSSPPairs = checkpoints.toList
      if(taskNameSSPPairs.isEmpty) {
        return null
      }

      // Need to turn taskNameSSPPairs List[(taskname, Map[SystemStreamPartition, Offset])] to Map[TaskName, Map[SSP, Offset]]
      taskNameSSPPairs                      // List[(taskname, Map[SystemStreamPartition, Offset])]
        .groupBy(_._1)                      // Group by taskname
        .mapValues(m => m.map(_._2))        // Drop the extra taskname that we grouped on
        .mapValues(m => m.reduce( _ ++ _))  // Merge all the maps of SSPs->Offset into one for the whole taskname
    }

    override def loadConfig(options: OptionSet): Config = {
      val config = super.loadConfig(options)
      if (options.has(newOffsetsOpt)) {
        val newOffsetsInputStream = new FileInputStream(options.valueOf(newOffsetsOpt))
        val properties = new Properties()

        properties.load(newOffsetsInputStream)
        newOffsetsInputStream.close()

        newOffsets = parseOffsets(properties)
      }
      config
    }
  }

  def apply(config: Config, offsets: TaskNameToCheckpointMap): CheckpointTool = {
    val metadataStore: CoordinatorStreamStore = new CoordinatorStreamStore(config, new MetricsRegistryMap())
    metadataStore.init()
    new CheckpointTool(offsets, metadataStore, config)
  }

  def main(args: Array[String]) {
    val cmdline = new CheckpointToolCommandLine
    val options = cmdline.parser.parse(args: _*)
    val userConfig = cmdline.loadConfig(options)
    val jobConfig = JobPlanner.generateSingleJobConfig(userConfig)
    val rewrittenConfig = ConfigUtil.rewriteConfig(jobConfig)
    info(s"Using the rewritten config: $rewrittenConfig")
    val tool = CheckpointTool(rewrittenConfig, cmdline.newOffsets)
    tool.run()
  }
}

class CheckpointTool(newOffsets: TaskNameToCheckpointMap, coordinatorStreamStore: CoordinatorStreamStore, userDefinedConfig: Config) extends Logging {

  def run() {
    val configFromCoordinatorStream: Config = getConfigFromCoordinatorStream(coordinatorStreamStore)

    println("Configuration read from the coordinator stream")
    println(configFromCoordinatorStream)

    val combinedConfigMap: util.Map[String, String] = new util.HashMap[String, String]()
    combinedConfigMap.putAll(configFromCoordinatorStream)
    combinedConfigMap.putAll(userDefinedConfig)
    val combinedConfig: Config = new MapConfig(combinedConfigMap)

    val taskConfig = new TaskConfig(combinedConfig)
    // Instantiate the checkpoint manager with coordinator stream configuration.
    val checkpointManager: CheckpointManager =
      JavaOptionals.toRichOptional(taskConfig.getCheckpointManager(new MetricsRegistryMap))
        .toOption
        .getOrElse(throw new SamzaException("Configuration: task.checkpoint.factory is not defined."))
    try {
      // Find all the TaskNames that would be generated for this job config
      val changelogManager = new ChangelogStreamManager(new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetChangelogMapping.TYPE))
      val jobModelManager = JobModelManager(combinedConfig, changelogManager.readPartitionMapping(),
        coordinatorStreamStore, new MetricsRegistryMap())
      val taskNames = jobModelManager
        .jobModel
        .getContainers
        .values
        .asScala
        .flatMap(_.getTasks.asScala.keys)
        .toSet

      taskNames.foreach(checkpointManager.register)
      checkpointManager.start()

      val lastCheckpoints = taskNames.map(taskName => {
        taskName -> Option(checkpointManager.readLastCheckpoint(taskName))
          .getOrElse(new Checkpoint(new java.util.HashMap[SystemStreamPartition, String]()))
          .getInputOffsets
          .asScala
          .toMap
      }).toMap

      lastCheckpoints.foreach(lcp => logCheckpoint(lcp._1, lcp._2, "Current checkpoint for task: "+ lcp._1))

      if (newOffsets != null) {
        newOffsets.foreach {
          case (taskName: TaskName, offsets: Map[SystemStreamPartition, String]) =>
            logCheckpoint(taskName, offsets, "New offset to be written for task: " + taskName)
            val checkpoint = new Checkpoint(offsets.asJava)
            checkpointManager.writeCheckpoint(taskName, checkpoint)
            info(s"Updated the checkpoint of the task: $taskName to: $offsets")
        }
      }
    } finally {
      checkpointManager.stop()
      coordinatorStreamStore.close()
   }
  }

  def getConfigFromCoordinatorStream(coordinatorStreamStore: CoordinatorStreamStore): Config = {
    CoordinatorStreamUtil.readConfigFromCoordinatorStream(coordinatorStreamStore)
  }

  def logCheckpoint(tn: TaskName, checkpoint: Map[SystemStreamPartition, String], prefix: String) {
    def logLine(tn:TaskName, ssp:SystemStreamPartition, offset:String) = (prefix + ": " + CheckpointTool.SSP_PATTERN + " = %s") format (tn.toString, ssp.getSystem, ssp.getStream, ssp.getPartition.getPartitionId, offset)

    checkpoint.keys.toList.sorted.foreach(ssp => info(logLine(tn, ssp, checkpoint(ssp))))
  }
}
