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

import kafka.api.{ TopicMetadataResponse, TopicMetadata }
import org.apache.samza.SamzaException
import kafka.client.ClientUtils
import java.util.concurrent.atomic.AtomicInteger

trait TopicMetadataStore extends Logging {
  def getTopicInfo(topics: Set[String]): Map[String, TopicMetadata]
}

class ClientUtilTopicMetadataStore(brokersListString: String, clientId: String, timeout: Int = 6000) extends TopicMetadataStore {
  val brokers = ClientUtils.parseBrokerList(brokersListString)
  var corrID = new AtomicInteger(0)

  def getTopicInfo(topics: Set[String]) = {
    val currCorrId = corrID.getAndIncrement

    debug("Fetching topic metadata.")
    val response: TopicMetadataResponse = ClientUtils.fetchTopicMetadata(topics, brokers, clientId, timeout, currCorrId)
    debug("Got topic metadata response: %s" format(response))

    if (response.correlationId != currCorrId) {
      throw new SamzaException("CorrelationID did not match for request on topics %s (sent %d, got %d)" format (topics, currCorrId, response.correlationId))
    }

    response.topicsMetadata
      .map(metadata => (metadata.topic, metadata))
      .toMap
  }
}
