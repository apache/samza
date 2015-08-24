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

package org.apache.samza.system.hdfs


import org.apache.samza.util.Logging
import org.apache.samza.system.{SystemAdmin, SystemStreamMetadata, SystemStreamPartition}


class HdfsSystemAdmin extends SystemAdmin with Logging {

  def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) = {
    new java.util.HashMap[SystemStreamPartition, String]()
  }

  def getSystemStreamMetadata(streamNames: java.util.Set[String]) = {
    new java.util.HashMap[String, SystemStreamMetadata]()
  }

  def createChangelogStream(topicName: String, numOfChangeLogPartitions: Int) = {
    throw new UnsupportedOperationException("Method not implemented.")
  }

  def validateChangelogStream(topicName: String, numOfChangeLogPartitions: Int) = {
    throw new UnsupportedOperationException("Method not implemented.")
  }

  def createCoordinatorStream(streamName: String) {
    throw new UnsupportedOperationException("Method not implemented.")
  }

  def offsetComparator(offset1: String, offset2: String) = {
    throw new UnsupportedOperationException("Method not implemented.")
  }
}
