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

import org.apache.samza.metrics.{MetricsRegistry, Counter}
import kafka.common.TopicAndPartition
import java.util.concurrent.ConcurrentHashMap
import grizzled.slf4j.Logging

/**
 * Wrapper around the metrics that BrokerProxies will be updating per topic partition.  Multiple BrokerProxies will
 * be updating the map at the same time, but no two BrokerProxies should be updating the same key at the same time.
 *
 * @param metricsRegistry Registry to hook counters into.
 */
private[kafka] class TopicAndPartitionMetrics(metricsRegistry:MetricsRegistry) extends Logging {
  val metricsGroup = "KafkaSystem"

  val counters = new ConcurrentHashMap[TopicAndPartition, (Counter,Counter,Counter)]()

  def addNewTopicAndPartition(tp:TopicAndPartition) = {
    if(containsTopicAndPartition(tp)) {
      warn("TopicAndPartitionsMetrics already has an entry for topic-partition %s, not adding." format tp)
    } else {
      counters.put(tp, (newCounter("%s-OffsetChange" format tp),  newCounter("%s-BytesRead" format tp), newCounter("%s-Reads" format tp)))
    }
  }

  def containsTopicAndPartition(tp:TopicAndPartition) = counters.containsKey(tp)

  def newCounter = metricsRegistry.newCounter(metricsGroup, _:String)

  def getOffsetCounter(tp:TopicAndPartition) = counters.get(tp)._1

  def getBytesReadCounter(tp:TopicAndPartition) = counters.get(tp)._2

  def getReadsCounter(tp:TopicAndPartition) = counters.get(tp)._3
}
