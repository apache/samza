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
package org.apache.samza.checkpoint.kafka

import java.util

import org.apache.samza.SamzaException
import org.apache.samza.container.TaskName
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConversions._

/**
 * Kafka Checkpoint Log-specific key used to identify what type of entry is
 * written for any particular log entry.
 *
 * @param map Backing map to hold key values
 */
class KafkaCheckpointLogKey private (val map: Map[String, String]) {
  // This might be better as a case class...
  import org.apache.samza.checkpoint.kafka.KafkaCheckpointLogKey._

  /**
   * Serialize this key to bytes
   * @return Key as bytes
   */
  def toBytes(): Array[Byte] = {
    val jMap = new util.HashMap[String, String](map.size)
    jMap.putAll(map)

    JSON_MAPPER.writeValueAsBytes(jMap)
  }

  private def getKey = map.getOrElse(CHECKPOINT_KEY_KEY, throw new SamzaException("No " + CHECKPOINT_KEY_KEY  + " in map for Kafka Checkpoint log key"))

  /**
   * Is this key for a checkpoint entry?
   *
   * @return true iff this key's entry is for a checkpoint
   */
  def isCheckpointKey = getKey.equals(CHECKPOINT_KEY_TYPE)

  /**
   * Is this key for a changelog partition mapping?
   *
   * @return true iff this key's entry is for a changelog partition mapping
   */
  @Deprecated
  def isChangelogPartitionMapping = getKey.equals(CHANGELOG_PARTITION_KEY_TYPE)

  /**
   * If this Key is for a checkpoint entry, return its associated TaskName.
   *
   * @return TaskName for this checkpoint or throw an exception if this key does not have a TaskName entry
   */
  def getCheckpointTaskName = {
    val asString = map.getOrElse(CHECKPOINT_TASKNAME_KEY, throw new SamzaException("No TaskName in checkpoint key: " + this))
    new TaskName(asString)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[KafkaCheckpointLogKey]

  override def equals(other: Any): Boolean = other match {
    case that: KafkaCheckpointLogKey =>
      (that canEqual this) &&
        map == that.map
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(map)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object KafkaCheckpointLogKey {
  /**
   *  Messages in the checkpoint log have keys associated with them. These keys are maps that describe the message's
   *  type, either a checkpoint or a changelog-partition-mapping.
   */
  val CHECKPOINT_KEY_KEY = "type"
  val CHECKPOINT_KEY_TYPE = "checkpoint"

  @Deprecated
  val CHANGELOG_PARTITION_KEY_TYPE = "changelog-partition-mapping"

  val CHECKPOINT_TASKNAME_KEY = "taskName"
  val SYSTEMSTREAMPARTITION_GROUPER_FACTORY_KEY = "systemstreampartition-grouper-factory"

  /**
   * Partition mapping keys have no dynamic values, so we just need one instance.
   */
  @Deprecated
  val CHANGELOG_PARTITION_MAPPING_KEY = new KafkaCheckpointLogKey(Map(CHECKPOINT_KEY_KEY -> CHANGELOG_PARTITION_KEY_TYPE))

  private val JSON_MAPPER = new ObjectMapper()
  val KEY_TYPEREFERENCE = new TypeReference[util.HashMap[String, String]]() {}

  var systemStreamPartitionGrouperFactoryString:Option[String] = None

  /**
   * Set the name of the factory configured to provide the SystemStreamPartition grouping
   * so it be included in the key.
   *
   * @param str Config value of SystemStreamPartition Grouper Factory
   */
  def setSystemStreamPartitionGrouperFactoryString(str:String) = {
    systemStreamPartitionGrouperFactoryString = Some(str)
  }

  /**
   * Get the name of the factory configured to provide the SystemStreamPartition grouping
   * so it be included in the key
   */
  def getSystemStreamPartitionGrouperFactoryString = systemStreamPartitionGrouperFactoryString.getOrElse(throw new SamzaException("No SystemStreamPartition grouping factory string has been set."))

  /**
   * Build a key for a a checkpoint log entry for a particular TaskName
   * @param taskName TaskName to build for this checkpoint entry
   *
   * @return Key for checkpoint log entry
   */
  def getCheckpointKey(taskName:TaskName) = {
    val map = Map(CHECKPOINT_KEY_KEY -> CHECKPOINT_KEY_TYPE,
      CHECKPOINT_TASKNAME_KEY -> taskName.getTaskName,
      SYSTEMSTREAMPARTITION_GROUPER_FACTORY_KEY -> getSystemStreamPartitionGrouperFactoryString)

    new KafkaCheckpointLogKey(map)
  }

  /**
   * Build a key for a changelog partition mapping entry
   *
   * @return Key for changelog partition mapping entry
   */
  @Deprecated
  def getChangelogPartitionMappingKey() = CHANGELOG_PARTITION_MAPPING_KEY

  /**
   * Deserialize a Kafka checkpoint log key
   * @param bytes Serialized (via JSON) Kafka checkpoint log key
   * @return Checkpoint log key
   */
  def fromBytes(bytes: Array[Byte]): KafkaCheckpointLogKey = {
    try {
      val jmap: util.HashMap[String, String] = JSON_MAPPER.readValue(bytes, KEY_TYPEREFERENCE)

      if(!jmap.containsKey(CHECKPOINT_KEY_KEY)) {
        throw new SamzaException("No type entry in checkpoint key: " + jmap)
      }

      // Only checkpoint keys have ssp grouper factory keys
      if(jmap.get(CHECKPOINT_KEY_KEY).equals(CHECKPOINT_KEY_TYPE)) {
        val sspGrouperFactory = jmap.get(SYSTEMSTREAMPARTITION_GROUPER_FACTORY_KEY)

        if (sspGrouperFactory == null) {
          throw new SamzaException("No SystemStreamPartition Grouper factory entry in checkpoint key: " + jmap)
        }

        if (!sspGrouperFactory.equals(getSystemStreamPartitionGrouperFactoryString)) {
          throw new DifferingSystemStreamPartitionGrouperFactoryValues(sspGrouperFactory, getSystemStreamPartitionGrouperFactoryString)
        }
      }

      new KafkaCheckpointLogKey(jmap.toMap)
    } catch {
      case e: Exception =>
        throw new SamzaException("Exception while deserializing checkpoint key", e)
    }
  }
}

class DifferingSystemStreamPartitionGrouperFactoryValues(inKey:String, inConfig:String) extends SamzaException {
  override def getMessage() = "Checkpoint key's SystemStreamPartition Grouper factory (" + inKey +
    ") does not match value from current configuration (" + inConfig + ").  " +
    "This likely means the SystemStreamPartitionGrouper was changed between job runs, which is not supported."
}