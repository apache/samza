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

package org.apache.samza.system.kafka

import org.apache.samza.Partition
import java.util.UUID
import org.apache.samza.util.ClientUtilTopicMetadataStore
import kafka.api.TopicMetadata
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemAdmin

class KafkaSystemAdmin(
  systemName: String,
  // TODO whenever Kafka decides to make the Set[Broker] class public, let's switch to Set[Broker] here.
  brokerListString: String,
  clientId: String = UUID.randomUUID.toString) extends SystemAdmin {

  def getPartitions(streamName: String): java.util.Set[Partition] = {
    val getTopicMetadata = (topics: Set[String]) => {
      new ClientUtilTopicMetadataStore(brokerListString, clientId)
        .getTopicInfo(topics)
    }

    val metadata = TopicMetadataCache.getTopicMetadata(
      Set(streamName),
      systemName,
      getTopicMetadata)

    metadata(streamName)
      .partitionsMetadata
      .map(pm => new Partition(pm.partitionId))
      .toSet[Partition]
  }
}
