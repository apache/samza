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

package org.apache.samza.system.kafka_deprecated

import org.apache.samza.metrics.MetricsHelper
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.MetricsRegistry
import java.util.concurrent.ConcurrentHashMap
import kafka.common.TopicAndPartition
import org.apache.samza.metrics.Counter
import org.apache.samza.metrics.Gauge

class KafkaSystemConsumerMetrics(val systemName: String = "unknown", val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {
  val offsets = new ConcurrentHashMap[TopicAndPartition, Counter]
  val bytesRead = new ConcurrentHashMap[TopicAndPartition, Counter]
  val reads = new ConcurrentHashMap[TopicAndPartition, Counter]
  val lag = new ConcurrentHashMap[TopicAndPartition, Gauge[Long]]
  val highWatermark = new ConcurrentHashMap[TopicAndPartition, Gauge[Long]]

  /*
   * (String, Int) = (host, port) of BrokerProxy.
   */

  val reconnects = new ConcurrentHashMap[(String, Int), Counter]
  val brokerBytesRead = new ConcurrentHashMap[(String, Int), Counter]
  val brokerReads = new ConcurrentHashMap[(String, Int), Counter]
  val brokerSkippedFetchRequests = new ConcurrentHashMap[(String, Int), Counter]
  val topicPartitions = new ConcurrentHashMap[(String, Int), Gauge[Int]]

  def registerTopicAndPartition(tp: TopicAndPartition) = {
    if (!offsets.contains(tp)) {
      offsets.put(tp, newCounter("%s-%s-offset-change" format (tp.topic, tp.partition)))
      bytesRead.put(tp, newCounter("%s-%s-bytes-read" format (tp.topic, tp.partition)))
      reads.put(tp, newCounter("%s-%s-messages-read" format (tp.topic, tp.partition)))
      highWatermark.put(tp, newGauge("%s-%s-high-watermark" format (tp.topic, tp.partition), -1L))
      lag.put(tp, newGauge("%s-%s-messages-behind-high-watermark" format (tp.topic, tp.partition), 0L))
    }
  }

  def registerBrokerProxy(host: String, port: Int) {
    reconnects.put((host, port), newCounter("%s-%s-reconnects" format (host, port)))
    brokerBytesRead.put((host, port), newCounter("%s-%s-bytes-read" format (host, port)))
    brokerReads.put((host, port), newCounter("%s-%s-messages-read" format (host, port)))
    brokerSkippedFetchRequests.put((host, port), newCounter("%s-%s-skipped-fetch-requests" format (host, port)))
    topicPartitions.put((host, port), newGauge("%s-%s-topic-partitions" format (host, port), 0))
  }

  // java friendlier interfaces
  // Gauges
  def setTopicPartitionValue(host: String, port: Int, value: Int) {
    topicPartitions.get((host,port)).set(value)
  }
  def setLagValue(topicAndPartition: TopicAndPartition, value: Long) {
    lag.get((topicAndPartition)).set(value);
  }
  def setHighWatermarkValue(topicAndPartition: TopicAndPartition, value: Long) {
    highWatermark.get((topicAndPartition)).set(value);
  }

  // Counters
  def incBrokerReads(host: String, port: Int) {
    brokerReads.get((host,port)).inc
  }
  def incReads(topicAndPartition: TopicAndPartition) {
    reads.get(topicAndPartition).inc;
  }
  def incBytesReads(topicAndPartition: TopicAndPartition, inc: Long) {
    bytesRead.get(topicAndPartition).inc(inc);
  }
  def incBrokerBytesReads(host: String, port: Int, incBytes: Long) {
    brokerBytesRead.get((host,port)).inc(incBytes)
  }
  def incBrokerSkippedFetchRequests(host: String, port: Int) {
    brokerSkippedFetchRequests.get((host,port)).inc()
  }
  def setOffsets(topicAndPartition: TopicAndPartition, offset: Long) {
    offsets.get(topicAndPartition).set(offset)
  }
  def incReconnects(host: String, port: Int) {
    reconnects.get((host,port)).inc()
  }
  override def getPrefix = systemName + "-"
}
