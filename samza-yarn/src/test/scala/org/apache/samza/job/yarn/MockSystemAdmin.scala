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

package org.apache.samza.job.yarn

import org.apache.samza.Partition
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.{SystemStreamMetadata, SystemStreamPartition, SystemAdmin}
import scala.collection.JavaConverters._

/**
 * A mock implementation class that returns metadata for each stream that contains numTasks partitions in it.
 */
class MockSystemAdmin(numTasks: Int) extends SystemAdmin {
  def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) = null
  def getSystemStreamMetadata(streamNames: java.util.Set[String]) = {
    streamNames.asScala.map(streamName => {
      val partitionMetadata = (0 until numTasks).map(partitionId => {
        new Partition(partitionId) -> new SystemStreamPartitionMetadata(null, null, null)
      }).toMap
      streamName -> new SystemStreamMetadata(streamName, partitionMetadata.asJava)
    }).toMap.asJava
  }

  override def createChangelogStream(topicName: String, numOfChangeLogPartitions: Int) {
    new UnsupportedOperationException("Method not implemented.")
  }

  override def validateChangelogStream(topicName: String, numOfChangeLogPartitions: Int) {
    new UnsupportedOperationException("Method not implemented.")
  }

  override def createCoordinatorStream(streamName: String) {
    new UnsupportedOperationException("Method not implemented.")
  }

  override def offsetComparator(offset1: String, offset2: String) = null
}
