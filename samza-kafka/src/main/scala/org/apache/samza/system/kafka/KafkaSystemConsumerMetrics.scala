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

import java.util.concurrent.ConcurrentHashMap

import kafka.common.TopicAndPartition
import org.apache.samza.metrics._

class KafkaSystemConsumerMetrics(val systemName: String = "unknown", val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {
  val offsets = new ConcurrentHashMap[TopicAndPartition, Counter]
  val bytesRead = new ConcurrentHashMap[TopicAndPartition, Counter]
  val reads = new ConcurrentHashMap[TopicAndPartition, Counter]
  val lag = new ConcurrentHashMap[TopicAndPartition, Gauge[Long]]
  val highWatermark = new ConcurrentHashMap[TopicAndPartition, Gauge[Long]]

  val clientBytesRead = new ConcurrentHashMap[String, Counter]
  val clientReads = new ConcurrentHashMap[String, Counter]
  val clientSkippedFetchRequests = new ConcurrentHashMap[String, Counter]
  val topicPartitions = new ConcurrentHashMap[String, Gauge[Int]]

  def registerTopicAndPartition(tp: TopicAndPartition) = {
    if (!offsets.contains(tp)) {
      offsets.put(tp, newCounter("%s-%s-offset-change" format(tp.topic, tp.partition)))
      bytesRead.put(tp, newCounter("%s-%s-bytes-read" format(tp.topic, tp.partition)))
      reads.put(tp, newCounter("%s-%s-messages-read" format(tp.topic, tp.partition)))
      highWatermark.put(tp, newGauge("%s-%s-high-watermark" format(tp.topic, tp.partition), -1L))
      lag.put(tp, newGauge("%s-%s-messages-behind-high-watermark" format(tp.topic, tp.partition), 0L))
    }
  }

  def registerClientProxy(clientName: String) {
    clientBytesRead.put(clientName, newCounter("%s-bytes-read" format clientName))
    clientReads.put((clientName), newCounter("%s-messages-read" format clientName))
    clientSkippedFetchRequests.put((clientName), newCounter("%s-skipped-fetch-requests" format clientName))
    topicPartitions.put(clientName, newGauge("%s-registered-topic-partitions" format clientName, 0))
  }

  // java friendlier interfaces
  // Gauges
  def setNumTopicPartitions(clientName: String, value: Int) {
    topicPartitions.get(clientName).set(value)
  }

  def setLagValue(topicAndPartition: TopicAndPartition, value: Long) {
    lag.get((topicAndPartition)).set(value);
  }

  def setHighWatermarkValue(topicAndPartition: TopicAndPartition, value: Long) {
    highWatermark.get((topicAndPartition)).set(value);
  }

  // Counters
  def incClientReads(clientName: String) {
    clientReads.get(clientName).inc
  }

  def incReads(topicAndPartition: TopicAndPartition) {
    reads.get(topicAndPartition).inc;
  }

  def incBytesReads(topicAndPartition: TopicAndPartition, inc: Long) {
    bytesRead.get(topicAndPartition).inc(inc);
  }

  def incClientBytesReads(clientName: String, incBytes: Long) {
    clientBytesRead.get(clientName).inc(incBytes)
  }

  def incClientSkippedFetchRequests(clientName: String) {
    clientSkippedFetchRequests.get(clientName).inc()
  }

  def setOffsets(topicAndPartition: TopicAndPartition, offset: Long) {
    offsets.get(topicAndPartition).set(offset)
  }

  override def getPrefix = systemName + "-"
}
