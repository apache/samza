/*
 *
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
 *
 */

package org.apache.samza.system.kafka

import kafka.consumer.SimpleConsumer
import kafka.api._
import kafka.common.TopicAndPartition
import kafka.common.TopicAndPartition

abstract class DefaultFetchSimpleConsumer(host: scala.Predef.String, port: scala.Int, soTimeout: scala.Int, bufferSize: scala.Int, clientId: scala.Predef.String)
  extends SimpleConsumer(host, port, soTimeout, bufferSize, clientId) {

  val maxWait:Int = Int.MaxValue
  val minBytes:Int = 1
  val fetchSize:Int

  def defaultFetch(fetches:(TopicAndPartition, Long)*) = {
    val fbr = new FetchRequestBuilder().maxWait(1000)
      .minBytes(minBytes)
      .clientId(clientId)

    fetches.foreach(f => fbr.addFetch(f._1.topic, f._1.partition, f._2, fetchSize))

    this.fetch(fbr.build())
  }

  override def close(): Unit = super.close()

  override def send(request: TopicMetadataRequest): TopicMetadataResponse = super.send(request)

  override def fetch(request: FetchRequest): FetchResponse = super.fetch(request)

  override def getOffsetsBefore(request: OffsetRequest): OffsetResponse = super.getOffsetsBefore(request)

  override def commitOffsets(request: OffsetCommitRequest): OffsetCommitResponse = super.commitOffsets(request)

  override def fetchOffsets(request: OffsetFetchRequest): OffsetFetchResponse = super.fetchOffsets(request)

  override def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = super.earliestOrLatestOffset(topicAndPartition, earliestOrLatest, consumerId)
}

