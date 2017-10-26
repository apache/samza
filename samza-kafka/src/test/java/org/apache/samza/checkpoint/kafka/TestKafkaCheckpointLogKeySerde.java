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
package org.apache.samza.checkpoint.kafka;

import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestKafkaCheckpointLogKeySerde {

  @Test
  public void testBinaryCompatibility() {
    KafkaCheckpointLogKey logKey1 = new KafkaCheckpointLogKey(GroupByPartitionFactory.class.getCanonicalName(),
        new TaskName("Partition 0"), KafkaCheckpointLogKey.CHECKPOINT_TYPE);
    KafkaCheckpointLogKeySerde checkpointSerde = new KafkaCheckpointLogKeySerde();

    byte[] bytes = ("{\"systemstreampartition-grouper-factory\"" +
        ":\"org.apache.samza.container.grouper.stream.GroupByPartitionFactory\",\"taskName\":\"Partition 0\"," +
        "\"type\":\"checkpoint\"}").getBytes();

    Assert.assertEquals(true, Arrays.equals(bytes, checkpointSerde.toBytes(logKey1)));
  }

  @Test
  public void testSerDe() {
    KafkaCheckpointLogKey logKey1 = new KafkaCheckpointLogKey(GroupByPartitionFactory.class.getCanonicalName(),
        new TaskName("Partition 0"), KafkaCheckpointLogKey.CHECKPOINT_TYPE);
    KafkaCheckpointLogKeySerde checkpointSerde = new KafkaCheckpointLogKeySerde();
    Assert.assertEquals(logKey1, checkpointSerde.fromBytes(checkpointSerde.toBytes(logKey1)));
  }
}
