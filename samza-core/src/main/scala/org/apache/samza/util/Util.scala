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

package org.apache.samza.util

import java.io._
import java.lang.management.ManagementFactory
import java.util
import java.util.Random
import java.net.URL
import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.CheckpointManagerFactory
import org.apache.samza.config.Config
import org.apache.samza.config.StorageConfig.Config2Storage
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.{SystemStreamPartition, SystemFactory, StreamMetadataCache, SystemStream}
import scala.collection.JavaConversions._
import scala.collection
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory
import org.apache.samza.container.TaskNamesToSystemStreamPartitions
import org.apache.samza.container.grouper.task.GroupByContainerCount

object Util extends Logging {
  val random = new Random

  /**
   * Make an environment variable string safe to pass.
   */
  def envVarEscape(str: String) = str.replace("\"", "\\\"").replace("'", "\\'")

  /**
   * Get a random number >= startInclusive, and < endExclusive.
   */
  def randomBetween(startInclusive: Int, endExclusive: Int) =
    startInclusive + random.nextInt(endExclusive - startInclusive)

  /**
   * Recursively remove a directory (or file), and all sub-directories. Equivalent
   * to rm -rf.
   */
  def rm(file: File) {
    if (file == null) {
      return
    } else if (file.isDirectory) {
      val files = file.listFiles()
      if (files != null) {
        for (f <- files)
          rm(f)
      }
      file.delete()
    } else {
      file.delete()
    }
  }

  /**
   * Instantiate a class instance from a given className.
   */
  def getObj[T](className: String) = {
    Class
      .forName(className)
      .newInstance
      .asInstanceOf[T]
  }

  /**
   * For each input stream specified in config, exactly determine its partitions, returning a set of SystemStreamPartitions
   * containing them all
   *
   * @param config Source of truth for systems and inputStreams
   * @return Set of SystemStreamPartitions, one for each unique system, stream and partition
   */
  def getInputStreamPartitions(config: Config): Set[SystemStreamPartition] = {
    val inputSystemStreams = config.getInputStreams
    val systemNames = config.getSystemNames.toSet

    // Map the name of each system to the corresponding SystemAdmin
    val systemAdmins = systemNames.map(systemName => {
      val systemFactoryClassName = config
        .getSystemFactory(systemName)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
      systemName -> systemFactory.getAdmin(systemName, config)
    }).toMap

    // Get the set of partitions for each SystemStream from the stream metadata
    new StreamMetadataCache(systemAdmins)
      .getStreamMetadata(inputSystemStreams)
      .flatMap { case (systemStream, metadata) =>
        metadata
          .getSystemStreamPartitionMetadata
          .keys
          .map(new SystemStreamPartition(systemStream, _))
      }.toSet
  }

  /**
   * Assign mapping of which TaskNames go to which container
   *
   * @param config For factories for Grouper and TaskNameGrouper
   * @param containerCount How many tasks are we we working with
   * @return Map of int (taskId) to SSPTaskNameMap that taskID is responsible for
   */
  def assignContainerToSSPTaskNames(config:Config, containerCount:Int): Map[Int, TaskNamesToSystemStreamPartitions] = {
    import org.apache.samza.config.JobConfig.Config2Job

    val allSystemStreamPartitions: Set[SystemStreamPartition] = Util.getInputStreamPartitions(config)

    val sspTaskNamesAsJava: util.Map[TaskName, util.Set[SystemStreamPartition]] = {
      val factoryString = config.getSystemStreamPartitionGrouperFactory

      info("Instantiating type " + factoryString + " to build SystemStreamPartition groupings")

      val factory = Util.getObj[SystemStreamPartitionGrouperFactory](factoryString)

      val grouper = factory.getSystemStreamPartitionGrouper(config)

      val groups = grouper.group(allSystemStreamPartitions)

      info("SystemStreamPartitionGrouper " + grouper + " has grouped the SystemStreamPartitions into the following taskNames:")
      groups.foreach(g => info("TaskName: " + g._1 + " => " + g._2))

      groups
    }

    val sspTaskNames = TaskNamesToSystemStreamPartitions(sspTaskNamesAsJava)

    info("Assigning " + sspTaskNames.keySet.size + " SystemStreamPartitions taskNames to " + containerCount + " containers.")

    // Here is where we should put in a pluggable option for the SSPTaskNameGrouper for locality, load-balancing, etc.
    val sspTaskNameGrouper = new GroupByContainerCount(containerCount)

    val containersToTaskNames = sspTaskNameGrouper.groupTaskNames(sspTaskNames).toMap

    info("Grouped SystemStreamPartition TaskNames (size = " + containersToTaskNames.size + "): ")
    containersToTaskNames.foreach(t => info("Container number: " + t._1 + " => " + t._2))

    containersToTaskNames
  }

  /**
   * Returns a SystemStream object based on the system stream name given. For
   * example, kafka.topic would return new SystemStream("kafka", "topic").
   */
  def getSystemStreamFromNames(systemStreamNames: String): SystemStream = {
    val idx = systemStreamNames.indexOf('.')
    if (idx < 0)
      throw new IllegalArgumentException("No '.' in stream name '" + systemStreamNames + "'. Stream names should be in the form 'system.stream'")
    new SystemStream(systemStreamNames.substring(0, idx), systemStreamNames.substring(idx + 1, systemStreamNames.length))
  }

  /**
   * Returns a SystemStream object based on the system stream name given. For
   * example, kafka.topic would return new SystemStream("kafka", "topic").
   */
  def getNameFromSystemStream(systemStream: SystemStream) = {
    systemStream.getSystem + "." + systemStream.getStream
  }

  /**
   * Using previous taskName to partition mapping and current taskNames for this job run, create a new mapping that preserves
   * the previous order and deterministically assigns any new taskNames to changelog partitions.  Be chatty about new or
   * missing taskNames.
   *
   * @param currentTaskNames All the taskNames the current job is processing
   * @param previousTaskNameMapping Previous mapping of taskNames to partition
   * @return New mapping of taskNames to partitions for the changelog
   */
  def resolveTaskNameToChangelogPartitionMapping(currentTaskNames:Set[TaskName],
    previousTaskNameMapping:Map[TaskName, Int]): Map[TaskName, Int] = {
    info("Previous mapping of taskNames to partition: " + previousTaskNameMapping.toList.sorted)
    info("Current set of taskNames: " + currentTaskNames.toList.sorted)

    val previousTaskNames: Set[TaskName] = previousTaskNameMapping.keySet

    if(previousTaskNames.equals(currentTaskNames)) {
      info("No change in TaskName sets from previous run. Returning previous mapping.")
      return previousTaskNameMapping
    }

    if(previousTaskNames.isEmpty) {
      warn("No previous taskName mapping defined.  This is OK if it's the first time the job is being run, otherwise data may have been lost.")
    }

    val missingTaskNames = previousTaskNames -- currentTaskNames

    if(missingTaskNames.isEmpty) {
      info("No taskNames are missing between this run and previous")
    } else {
      warn("The following taskNames were previously defined and are no longer present: " + missingTaskNames)
    }

    val newTaskNames = currentTaskNames -- previousTaskNames

    if(newTaskNames.isEmpty) {
      info("No new taskNames have been added between this run and the previous")
      previousTaskNameMapping // Return the old mapping since there are no new taskNames for which to account

    } else {
      warn("The following new taskNames have been added in this job run: " + newTaskNames)

      // Sort the new taskNames and assign them partitions (starting at 0 for now)
      val sortedNewTaskNames = newTaskNames.toList.sortWith { (a,b) => a.getTaskName < b.getTaskName }.zipWithIndex.toMap

      // Find next largest partition to use based on previous mapping
      val nextPartitionToUse = if(previousTaskNameMapping.size == 0) 0
                               else previousTaskNameMapping.foldLeft(0)((a,b) => math.max(a, b._2)) + 1

      // Bump up the partition values
      val newTaskNamesWithTheirPartitions = sortedNewTaskNames.map(c => c._1 -> (c._2 + nextPartitionToUse))
      
      // Merge old and new
      val newMapping = previousTaskNameMapping ++ newTaskNamesWithTheirPartitions
      
      info("New taskName to partition mapping: " + newMapping.toList.sortWith{ (a,b) => a._2 < b._2})
      
      newMapping
    }
  }

  /**
   * Read the TaskName to changelog partition mapping from the checkpoint manager, if one exists.
   *
   * @param config To pull out values for instantiating checkpoint manager
   * @param tasksToSSPTaskNames Current TaskNames for the current job run
   * @return Current mapping of TaskNames to changelog partitions
   */
  def getTaskNameToChangeLogPartitionMapping(config: Config, tasksToSSPTaskNames: Map[Int, TaskNamesToSystemStreamPartitions]) = {
    val taskNameMaps: Set[TaskNamesToSystemStreamPartitions] = tasksToSSPTaskNames.map(_._2).toSet
    val currentTaskNames: Set[TaskName] = taskNameMaps.map(_.keys).toSet.flatten

    // We need to oh so quickly instantiate a checkpoint manager and grab the partition mapping from the log, then toss the manager aside
    val checkpointManager = config.getCheckpointManagerFactory match {
      case Some(checkpointFactoryClassName) =>
        Util
          .getObj[CheckpointManagerFactory](checkpointFactoryClassName)
          .getCheckpointManager(config, new MetricsRegistryMap)
      case _ => null
    }

    if(checkpointManager == null) {
      // Check if we have a changelog configured, which requires a checkpoint manager

      if(!config.getStoreNames.isEmpty) {
        throw new SamzaException("Storage factories configured, but no checkpoint manager has been specified.  " +
          "Unable to start job as there would be no place to store changelog partition mapping.")
      }
      // No need to do any mapping, just use what has been provided
      Util.resolveTaskNameToChangelogPartitionMapping(currentTaskNames, Map[TaskName, Int]())
    } else {

      info("Got checkpoint manager: %s" format checkpointManager)

      // Always put in a call to create so the log is available for the tasks on startup.
      // Reasonably lame to hide it in here.  TODO: Pull out to more visible location.
      checkpointManager.start

      val previousMapping: Map[TaskName, Int] = {
        val fromCM = checkpointManager.readChangeLogPartitionMapping()

        fromCM.map(kv => kv._1 -> kv._2.intValue()).toMap // Java to Scala interop!!!
      }

      checkpointManager.stop

      val newMapping = Util.resolveTaskNameToChangelogPartitionMapping(currentTaskNames, previousMapping)

      if (newMapping != null) {
        info("Writing new changelog partition mapping to checkpoint manager.")
        checkpointManager.writeChangeLogPartitionMapping(newMapping.map(kv => kv._1 -> new java.lang.Integer(kv._2))) //Java to Scala interop!!!
      }

      newMapping
    }
  }

  /**
   * Makes sure that an object is not null, and throws a NullPointerException
   * if it is.
   */
  def notNull[T](obj: T, msg: String) = if (obj == null) {
    throw new NullPointerException(msg)
  }
  
  /**
   * Returns the name representing the JVM. It usually contains the PID of the process plus some additional information
   * @return String that contains the name representing this JVM
   */
  def getContainerPID(): String = {
    ManagementFactory.getRuntimeMXBean().getName()
  }

  /**
   * Reads a URL and returns its body as a string. Does no error handling.
   *
   * @param url HTTP URL to read from.
   * @return String payload of the body of the HTTP response.
   */
  def read(url: URL): String = {
    val conn = url.openConnection();
    val br = new BufferedReader(new InputStreamReader(conn.getInputStream));
    var line: String = null;
    val body = Iterator.continually(br.readLine()).takeWhile(_ != null).mkString
    br.close
    body
  }
}
