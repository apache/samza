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
package org.apache.samza.system.kafka;

import java.util.HashMap;
import java.util.Map;
import kafka.common.TopicAndPartition;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;


public class TestKafkaSystemConsumerMetrics {
  @Test
  public void testKafkaSystemConsumerMetrics() {
    String systemName = "system";
    TopicAndPartition tp1 = new TopicAndPartition("topic1", 1);
    TopicAndPartition tp2 = new TopicAndPartition("topic2", 2);
    String clientName = "clientName";

    // record expected values for further comparison
    Map<String, String> expectedValues = new HashMap<>();

    ReadableMetricsRegistry registry = new MetricsRegistryMap();
    KafkaSystemConsumerMetrics metrics = new KafkaSystemConsumerMetrics(systemName, registry);

    // initialize the metrics for the partitions
    metrics.registerTopicAndPartition(tp1);
    metrics.registerTopicAndPartition(tp2);

    // initialize the metrics for the host:port
    metrics.registerClientProxy(clientName);

    metrics.setOffsets(tp1, 1001);
    metrics.setOffsets(tp2, 1002);
    expectedValues.put(metrics.offsets().get(tp1).getName(), "1001");
    expectedValues.put(metrics.offsets().get(tp2).getName(), "1002");

    metrics.incBytesReads(tp1, 10);
    metrics.incBytesReads(tp1, 5); // total 15
    expectedValues.put(metrics.bytesRead().get(tp1).getName(), "15");

    metrics.incReads(tp1);
    metrics.incReads(tp1); // total 2
    expectedValues.put(metrics.reads().get(tp1).getName(), "2");

    metrics.setHighWatermarkValue(tp2, 1000);
    metrics.setHighWatermarkValue(tp2, 1001); // final value 1001
    expectedValues.put(metrics.highWatermark().get(tp2).getName(), "1001");

    metrics.setLagValue(tp1, 200);
    metrics.setLagValue(tp1, 201); // final value 201
    expectedValues.put(metrics.lag().get(tp1).getName(), "201");

    metrics.incClientBytesReads(clientName, 100); // broker-bytes-read
    metrics.incClientBytesReads(clientName, 110); // total 210
    expectedValues.put(metrics.clientBytesRead().get(clientName).getName(), "210");

    metrics.incClientReads(clientName); // messages-read
    metrics.incClientReads(clientName); // total 2
    expectedValues.put(metrics.clientReads().get(clientName).getName(), "2");

    metrics.setNumTopicPartitions(clientName, 2); // "topic-partitions"
    metrics.setNumTopicPartitions(clientName, 3); // final value 3
    expectedValues.put(metrics.topicPartitions().get(clientName).getName(), "3");


    String groupName = metrics.group();
    Assert.assertEquals(groupName, KafkaSystemConsumerMetrics.class.getName());
    Assert.assertEquals(metrics.systemName(), systemName);

    Map<String, Metric> metricMap = registry.getGroup(groupName);
    validate(metricMap, expectedValues);
  }

  protected static void validate(Map<String, Metric> metricMap, Map<String, String> expectedValues) {
    // match the expected value, set in the test above, and the value in the metrics
    for (Map.Entry<String, String> e : expectedValues.entrySet()) {
      String metricName = e.getKey();
      String expectedValue = e.getValue();
      // get the metric from the registry
      String actualValue = metricMap.get(metricName).toString();

      //System.out.println("name=" + metricName + " expVal="  + expectedValue + " actVal=" + actualValue);
      Assert.assertEquals("failed for metricName=" + metricName, actualValue, expectedValue);
    }
  }
}
