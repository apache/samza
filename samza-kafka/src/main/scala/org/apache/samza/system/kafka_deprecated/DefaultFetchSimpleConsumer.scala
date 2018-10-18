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

package org.apache.samza.system.kafka_deprecated

import kafka.consumer.SimpleConsumer
import kafka.api._
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig

class DefaultFetchSimpleConsumer(host: scala.Predef.String, port: scala.Int, soTimeout: scala.Int, bufferSize: scala.Int,
  clientId: scala.Predef.String, fetchSize: StreamFetchSizes = new StreamFetchSizes,
  minBytes: Int = ConsumerConfig.MinFetchBytes, maxWait: Int = ConsumerConfig.MaxFetchWaitMs)
  extends SimpleConsumer(host, port, soTimeout, bufferSize, clientId) {

  def defaultFetch(fetches: (TopicAndPartition, Long)*) = {
    val fbr = new FetchRequestBuilder().maxWait(maxWait)
      .minBytes(minBytes)
      .clientId(clientId)

    fetches.foreach(f => fbr.addFetch(f._1.topic, f._1.partition, f._2, fetchSize.streamValue.getOrElse(f._1.topic, fetchSize.defaultValue)))

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

/**
 * a simple class for holding values for the stream's fetch size (fetch.message.max.bytes).
 * The stream-level fetch size values are put in the streamValue map streamName -> fetchSize.
 * If stream-level fetch size is not defined, use the default value. The default value is the
 * Kafka's default fetch size value or the system-level fetch size value (if defined).
 */
case class StreamFetchSizes(defaultValue: Int = ConsumerConfig.MaxFetchSize, streamValue: Map[String, Int] = Map[String, Int]())

