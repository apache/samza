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

import java.io.File
import java.util.Random
import grizzled.slf4j.Logging
import org.apache.samza.{ Partition, SamzaException }
import org.apache.samza.config.Config
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.TaskConfig.Config2Task
import scala.collection.JavaConversions._
import org.apache.samza.system.{ SystemStreamPartition, SystemFactory, SystemStream }
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.`type`.TypeReference
import java.util


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
   * corresponding to them all
   *
   * @param config Source of truth for systems and inputStreams
   * @return Set of SystemStreamPartitions, one for each unique system, stream and partition
   */
  def getInputStreamPartitions(config: Config): Set[SystemStreamPartition] = {
    val inputSystemStreams = config.getInputStreams
    val systemNames = config.getSystemNames.toSet

    systemNames.flatMap(systemName => {
      // Get SystemAdmin for system.
      val systemFactoryClassName = config
        .getSystemFactory(systemName)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
      val systemAdmin = systemFactory.getAdmin(systemName, config)

      // Get metadata for every stream that belongs to this system.
      val streamNames = inputSystemStreams
        .filter(_.getSystem.equals(systemName))
        .map(_.getStream)
      val systemStreamMetadata = systemAdmin.getSystemStreamMetadata(streamNames)

      // Get a set of all SSPs for every stream that belongs to this system.
      systemStreamMetadata
        .values
        .flatMap(systemStreamMetadata => {
          val streamName = systemStreamMetadata.getStreamName
          val systemStreamPartitionSet = systemStreamMetadata.getSystemStreamPartitionMetadata.keys
          systemStreamPartitionSet.map(new SystemStreamPartition(systemName, streamName, _)).toSet
        })
    })
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
   * For specified containerId, create a list of of the streams and partitions that task should handle,
   * based on the number of tasks in the job
   *
   * @param containerId TaskID to determine work for
   * @param containerCount Total number of tasks in the job
   * @param ssp All SystemStreamPartitions
   * @return Collection of streams and partitions for this particular containerId
   */
  def getStreamsAndPartitionsForContainer(containerId: Int, containerCount: Int, ssp: Set[SystemStreamPartition]): Set[SystemStreamPartition] = {
    ssp.filter(_.getPartition.getPartitionId % containerCount == containerId)
  }

  /**
   * Jackson really hates Scala's classes, so we need to wrap up the SSP in a form Jackson will take
   */
  private class SSPWrapper(@scala.beans.BeanProperty var partition:java.lang.Integer = null,
                           @scala.beans.BeanProperty var Stream:java.lang.String = null,
                           @scala.beans.BeanProperty var System:java.lang.String = null) {
    def this() { this(null, null, null) }
    def this(ssp:SystemStreamPartition) { this(ssp.getPartition.getPartitionId, ssp.getSystemStream.getStream, ssp.getSystemStream.getSystem)}
  }

  def serializeSSPSetToJSON(ssps: Set[SystemStreamPartition]): String = {
    val al = new util.ArrayList[SSPWrapper](ssps.size)
    for(ssp <- ssps) { al.add(new SSPWrapper(ssp)) }

    new ObjectMapper().writeValueAsString(al)
  }

  def deserializeSSPSetFromJSON(ssp: String) = {
    val om = new ObjectMapper()

    val asWrapper = om.readValue(ssp, new TypeReference[util.ArrayList[SSPWrapper]]() { }).asInstanceOf[util.ArrayList[SSPWrapper]]
    asWrapper.map(w => new SystemStreamPartition(w.getSystem, w.getStream(), new Partition(w.getPartition()))).toSet
  }

  /**
   * Makes sure that an object is not null, and throws a NullPointerException
   * if it is.
   */
  def notNull[T](obj: T, msg: String) = if (obj == null) {
    throw new NullPointerException(msg)
  }
}
