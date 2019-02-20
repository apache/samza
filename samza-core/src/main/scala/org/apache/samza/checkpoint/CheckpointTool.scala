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
import java.util
import java.util.regex.Pattern
import joptsimple.ArgumentAcceptingOptionSpec
import joptsimple.OptionSet
import org.apache.samza.checkpoint.CheckpointTool.TaskNameToCheckpointMap
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config._
import org.apache.samza.container.TaskName
import org.apache.samza.job.JobRunner.{info, warn, _}
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.CommandLine
import org.apache.samza.util.Logging
import org.apache.samza.util.Util
import org.apache.samza.Partition
import org.apache.samza.SamzaException
import scala.collection.JavaConverters._
import org.apache.samza.coordinator.JobModelManager
import org.apache.samza.coordinator.stream.CoordinatorStreamManager
import org.apache.samza.storage.ChangelogStreamManager
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
    val newOffsetsOpt: ArgumentAcceptingOptionSpec[URI] =
      parser.accepts("new-offsets", "URI of file (e.g. file:///some/local/path.properties) " +
                                    "containing offsets to write to the job's checkpoint topic. " +
                                    "If not given, this tool prints out the current offsets.")
            .withRequiredArg
            .ofType(classOf[URI])
            .describedAs("path")

    var newOffsets: TaskNameToCheckpointMap = _

    def parseOffsets(propertiesFile: Config): TaskNameToCheckpointMap = {
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

    override def loadConfig(options: OptionSet): MapConfig = {
      val config = super.loadConfig(options)
      if (options.has(newOffsetsOpt)) {
        val properties = configFactory.getConfig(options.valueOf(newOffsetsOpt))
        newOffsets = parseOffsets(properties)
      }
      config
    }

    def genJobConfigs(config: MapConfig): MapConfig = {
      var genConfig = new util.HashMap[String, String](config)

      if (genConfig.containsKey(JobConfig.JOB_ID))
        warn("%s is a deprecated configuration, use %s instead." format(JobConfig.JOB_ID, ApplicationConfig.APP_ID))

      if (genConfig.containsKey(JobConfig.JOB_NAME))
        warn("%s is a deprecated configuration, use %s instead." format(JobConfig.JOB_NAME, ApplicationConfig.APP_NAME))

      if (genConfig.containsKey(ApplicationConfig.APP_NAME)) {
        val appName = genConfig.get(ApplicationConfig.APP_NAME)
        info("app.name is defined, setting job.name equal to app.name value: %s" format(appName))
        genConfig.put(JobConfig.JOB_NAME, appName)
      }

      if (genConfig.containsKey(ApplicationConfig.APP_ID)) {
        val appId = genConfig.get(ApplicationConfig.APP_ID)
        info("app.id is defined, setting job.id equal to app.name value: %s" format(appId))
        genConfig.put(JobConfig.JOB_ID, appId)
      }
      new MapConfig(genConfig)
    }
  }

  def apply(config: Config, offsets: TaskNameToCheckpointMap): CheckpointTool = {
    val coordinatorStreamManager = new CoordinatorStreamManager(config, new MetricsRegistryMap())
    new CheckpointTool(offsets, coordinatorStreamManager)
  }

  def rewriteConfig(config: JobConfig): Config = {
    def rewrite(c: JobConfig, rewriterName: String): Config = {
      val rewriterClassName = config
              .getConfigRewriterClass(rewriterName)
              .getOrElse(throw new SamzaException("Config rewriter class: %s not found." format rewriterName))
      val rewriter = Util.getObj(rewriterClassName, classOf[ConfigRewriter])
      info("Re-writing config for CheckpointTool with " + rewriter)
      rewriter.rewrite(rewriterName, c)
    }

    config.getConfigRewriters match {
      case Some(rewriters) => rewriters.split(",").foldLeft(config)(rewrite)
      case _ => config
    }
  }

  def main(args: Array[String]) {
    val cmdline = new CheckpointToolCommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    val mergedConfig = cmdline.genJobConfigs(config)
    val rewrittenConfig = rewriteConfig(new JobConfig(mergedConfig))
    info(s"Using the rewritten config: $rewrittenConfig")
    val tool = CheckpointTool(rewrittenConfig, cmdline.newOffsets)
    tool.run()
  }
}

class CheckpointTool(newOffsets: TaskNameToCheckpointMap, coordinatorStreamManager: CoordinatorStreamManager) extends Logging {

  def run() {
    // Read the configuration stored in the coordinator stream.
    coordinatorStreamManager.register(getClass.getSimpleName)
    coordinatorStreamManager.start()
    coordinatorStreamManager.bootstrap()
    val configFromCoordinatorStream: Config = coordinatorStreamManager.getConfig

    // Instantiate the checkpoint manager with coordinator stream configuration.
    val checkpointManager: CheckpointManager = configFromCoordinatorStream.getCheckpointManagerFactory() match {
      case Some(className) =>
        Util.getObj(className, classOf[CheckpointManagerFactory])
          .getCheckpointManager(configFromCoordinatorStream, new MetricsRegistryMap)
      case _ =>
        throw new SamzaException("Configuration: task.checkpoint.factory is not defined.")
    }

    // Find all the TaskNames that would be generated for this job config
    val changelogManager = new ChangelogStreamManager(coordinatorStreamManager)
    val jobModelManager = JobModelManager(configFromCoordinatorStream, changelogManager.readPartitionMapping())
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
                                          .getOffsets
                                          .asScala
                                          .toMap
    }).toMap

    lastCheckpoints.foreach(lcp => logCheckpoint(lcp._1, lcp._2, "Current checkpoint for task: "+ lcp._1))

    if (newOffsets != null) {
      newOffsets.foreach {
        case (taskName: TaskName, offsets: Map[SystemStreamPartition, String]) => {
          logCheckpoint(taskName, offsets, "New offset to be written for task: " + taskName)
          val checkpoint = new Checkpoint(offsets.asJava)
          checkpointManager.writeCheckpoint(taskName, checkpoint)
          info(s"Updated the checkpoint of the task: $taskName to: $offsets")
        }
      }
    }

    checkpointManager.stop()
    coordinatorStreamManager.stop()
  }

  def logCheckpoint(tn: TaskName, checkpoint: Map[SystemStreamPartition, String], prefix: String) {
    def logLine(tn:TaskName, ssp:SystemStreamPartition, offset:String) = (prefix + ": " + CheckpointTool.SSP_PATTERN + " = %s") format (tn.toString, ssp.getSystem, ssp.getStream, ssp.getPartition.getPartitionId, offset)

    checkpoint.keys.toList.sorted.foreach(ssp => info(logLine(tn, ssp, checkpoint(ssp))))
  }
}
