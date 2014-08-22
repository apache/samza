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
package org.apache.samza.container

import org.apache.samza.util.Logging
import org.apache.samza.SamzaException
import org.apache.samza.system.{SystemStream, SystemStreamPartition}
import scala.collection.{immutable, Map, MapLike}

/**
 * Map of {@link TaskName} to its set of {@link SystemStreamPartition}s with additional methods for aggregating
 * those SystemStreamPartitions' individual system, streams and partitions.  Is useful for highlighting this
 * particular, heavily used map within the code.
 *
 * @param m Original map of TaskNames to SystemStreamPartitions
 */
class TaskNamesToSystemStreamPartitions(m:Map[TaskName, Set[SystemStreamPartition]] = Map[TaskName, Set[SystemStreamPartition]]())
  extends Map[TaskName, Set[SystemStreamPartition]]
  with MapLike[TaskName, Set[SystemStreamPartition], TaskNamesToSystemStreamPartitions] with Logging {

  // Constructor
  validate

  // Methods

  // TODO: Get rid of public constructor, rely entirely on the companion object
  override def -(key: TaskName): TaskNamesToSystemStreamPartitions = new TaskNamesToSystemStreamPartitions(m - key)

  override def +[B1 >: Set[SystemStreamPartition]](kv: (TaskName, B1)): Map[TaskName, B1] = new TaskNamesToSystemStreamPartitions(m + kv.asInstanceOf[(TaskName, Set[SystemStreamPartition])])

  override def iterator: Iterator[(TaskName, Set[SystemStreamPartition])] = m.iterator

  override def get(key: TaskName): Option[Set[SystemStreamPartition]] = m.get(key)

  override def empty: TaskNamesToSystemStreamPartitions = new TaskNamesToSystemStreamPartitions()

  override def seq: Map[TaskName, Set[SystemStreamPartition]] = m.seq

  override def foreach[U](f: ((TaskName, Set[SystemStreamPartition])) => U): Unit = m.foreach(f)

  override def size: Int = m.size

  /**
   * Validate that this is a legal mapping of TaskNames to SystemStreamPartitions.  At the moment,
   * we only check that an SSP is included in the mapping at most once.  We could add other,
   * pluggable validations here, or if we decided to allow an SSP to appear in the mapping more than
   * once, remove this limitation.
   */
  def validate():Unit = {
    // Convert sets of SSPs to lists, to preserve duplicates
    val allSSPs: List[SystemStreamPartition] = m.values.toList.map(_.toList).flatten
    val sspCountMap = allSSPs.groupBy(ssp => ssp)  // Group all the SSPs together
      .map(ssp => (ssp._1 -> ssp._2.size))         // Turn into map -> count of that SSP
      .filter(ssp => ssp._2 != 1)                  // Filter out those that appear once

    if(!sspCountMap.isEmpty) {
      throw new SamzaException("Assigning the same SystemStreamPartition to multiple TaskNames is not currently supported." +
        "  Out of compliance SystemStreamPartitions and counts: " + sspCountMap)
    }

    debug("Successfully validated TaskName to SystemStreamPartition set mapping:" + m)
  }

  /**
   * Return a set of all the SystemStreamPartitions for all the keys.
   *
   * @return All SystemStreamPartitions within this map
   */
  def getAllSSPs(): Iterable[SystemStreamPartition] = m.values.flatten

  /**
   * Return a set of all the Systems presents in the SystemStreamPartitions across all the keys
   *
   * @return All Systems within this map
   */
  def getAllSystems(): Set[String] = getAllSSPs.map(_.getSystemStream.getSystem).toSet

  /**
   * Return a set of all the Partition IDs in the SystemStreamPartitions across all the keys
   *
   * @return All Partition IDs within this map
   */
  def getAllPartitionIds(): Set[Int] = getAllSSPs.map(_.getPartition.getPartitionId).toSet

  /**
   * Return a set of all the Streams in the SystemStreamPartitions across all the keys
   *
   * @return All Streams within this map
   */
  def getAllStreams(): Set[String] = getAllSSPs.map(_.getSystemStream.getStream).toSet

  /**
   * Return a set of all the SystemStreams in the SystemStreamPartitions across all the keys
   *
   * @return All SystemStreams within this map
   */
  def getAllSystemStreams: Set[SystemStream] = getAllSSPs().map(_.getSystemStream).toSet

  // CommandBuilder needs to get a copy of this map and is a Java interface, therefore we can't just go straight
  // from this type to JSON (for passing into the command option.
  // Not super crazy about having the Java -> Scala and Scala -> Java methods in two different (but close) places:
  // here and in the apply method on the companion object.  May be better to just have a conversion util, but would
  // be less clean.  Life is cruel on the border of Scalapolis and Javatown.
  def getJavaFriendlyType: java.util.Map[TaskName, java.util.Set[SystemStreamPartition]] = {
    import scala.collection.JavaConverters._

    m.map({case(k,v) => k -> v.asJava}).toMap.asJava
  }
}

object TaskNamesToSystemStreamPartitions {
  def apply() = new TaskNamesToSystemStreamPartitions()

  def apply(m: Map[TaskName, Set[SystemStreamPartition]]) = new TaskNamesToSystemStreamPartitions(m)

  /**
   * Convert from Java-happy type we obtain from the SSPTaskName factory
   *
   * @param m Java version of a map of sets of strings
   * @return Populated SSPTaskName map
   */
  def apply(m: java.util.Map[TaskName, java.util.Set[SystemStreamPartition]]) = {
    import scala.collection.JavaConversions._

    val rightType: immutable.Map[TaskName, Set[SystemStreamPartition]] = m.map({case(k,v) => k -> v.toSet}).toMap

    new TaskNamesToSystemStreamPartitions(rightType)
  }
}
