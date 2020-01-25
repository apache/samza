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
package org.apache.samza.util;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.Partition;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;


public class TestKafkaUtil {
  @Test
  public void testGetIntegerPartitionKey() {
    SystemStream systemStream = new SystemStream("system", "stream");
    int partitionKeyInt = 10;
    OutgoingMessageEnvelope outgoingMessageEnvelope =
        new OutgoingMessageEnvelope(systemStream, partitionKeyInt, "key", "message");
    assertEquals(0, KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(1)).intValue());
    assertEquals(Integer.hashCode(partitionKeyInt) % 3,
        KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(3)).intValue());
    assertEquals(Integer.hashCode(partitionKeyInt) % 8,
        KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(8)).intValue());

    partitionKeyInt = -10;
    outgoingMessageEnvelope = new OutgoingMessageEnvelope(systemStream, partitionKeyInt, "key", "message");
    assertEquals(0, KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(1)).intValue());
    assertEquals(Integer.hashCode(Math.abs(partitionKeyInt)) % 3,
        KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(3)).intValue());
    assertEquals(Integer.hashCode(Math.abs(partitionKeyInt)) % 8,
        KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(8)).intValue());

    // KafkaUtil uses 0 when partition key is MIN_VALUE
    partitionKeyInt = Integer.MIN_VALUE;
    outgoingMessageEnvelope = new OutgoingMessageEnvelope(systemStream, partitionKeyInt, "key", "message");
    assertEquals(0, KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(1)).intValue());
    assertEquals(0, KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(3)).intValue());
    assertEquals(0, KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(8)).intValue());

    String partitionKeyString = "abc"; // hash code is 96353
    outgoingMessageEnvelope = new OutgoingMessageEnvelope(systemStream, partitionKeyString, "key", "message");
    assertEquals(0, KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(1)).intValue());
    assertEquals(Math.abs(partitionKeyString.hashCode()) % 3,
        KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(3)).intValue());
    assertEquals(Math.abs(partitionKeyString.hashCode()) % 8,
        KafkaUtil.getIntegerPartitionKey(outgoingMessageEnvelope, partitionInfoList(8)).intValue());
  }

  @Test
  public void testGetCheckpointTopic() {
    assertEquals("__samza_checkpoint_ver_1_for_myJob_myId",
        KafkaUtil.getCheckpointTopic("myJob", "myId", new MapConfig()));
    assertEquals("__samza_checkpoint_ver_1_for_my-job-with-underscore_my-id-with-underscore",
        KafkaUtil.getCheckpointTopic("my_job-with_underscore", "my_id-with_underscore", new MapConfig()));

    Config configWithBatchMode = new MapConfig(ImmutableMap.of(
        ApplicationConfig.APP_MODE, ApplicationConfig.ApplicationMode.BATCH.name(),
        ApplicationConfig.APP_RUN_ID, "myRunId"
    ));
    assertEquals("__samza_checkpoint_ver_1_for_myJob_myId-myRunId",
        KafkaUtil.getCheckpointTopic("myJob", "myId", configWithBatchMode));
    assertEquals("__samza_checkpoint_ver_1_for_my-job-with-underscore_my-id-with-underscore-myRunId",
        KafkaUtil.getCheckpointTopic("my_job-with_underscore", "my_id-with_underscore", configWithBatchMode));
  }

  @Test
  public void testToSystemStreamPartition() {
    String system = "mySystem", stream = "myStream";
    int partition = 3;
    TopicPartition topicPartition = new TopicPartition(stream, partition);
    assertEquals(new SystemStreamPartition(system, stream, new Partition(partition)),
        KafkaUtil.toSystemStreamPartition(system, topicPartition));
  }

  private static List<PartitionInfo> partitionInfoList(int numPartitions) {
    return IntStream.range(0, numPartitions).mapToObj(i -> mock(PartitionInfo.class)).collect(Collectors.toList());
  }
}
