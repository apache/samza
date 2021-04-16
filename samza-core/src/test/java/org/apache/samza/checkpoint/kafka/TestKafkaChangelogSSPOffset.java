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

import org.apache.samza.checkpoint.CheckpointId;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class TestKafkaChangelogSSPOffset {
  @Test
  public void testSerializeDeserialize() {
    KafkaChangelogSSPOffset kafkaChangelogSSPOffset = new KafkaChangelogSSPOffset(CheckpointId.create(), "offset");
    KafkaChangelogSSPOffset deserializedKafkaChangelogSSPOffset = KafkaChangelogSSPOffset.fromString(kafkaChangelogSSPOffset.toString());

    assertEquals(kafkaChangelogSSPOffset.getCheckpointId(), deserializedKafkaChangelogSSPOffset.getCheckpointId());
    assertEquals("offset", deserializedKafkaChangelogSSPOffset.getChangelogOffset());
    assertEquals(kafkaChangelogSSPOffset, deserializedKafkaChangelogSSPOffset);
  }

  @Test
  public void testSerializeDeserializeNullOffsets() {
    KafkaChangelogSSPOffset kafkaChangelogSSPOffset = new KafkaChangelogSSPOffset(CheckpointId.create(), null);
    KafkaChangelogSSPOffset deserializedKafkaChangelogSSPOffset = KafkaChangelogSSPOffset.fromString(kafkaChangelogSSPOffset.toString());

    assertEquals(kafkaChangelogSSPOffset.getCheckpointId(), deserializedKafkaChangelogSSPOffset.getCheckpointId());
    assertNull(deserializedKafkaChangelogSSPOffset.getChangelogOffset());
    assertEquals(kafkaChangelogSSPOffset, deserializedKafkaChangelogSSPOffset);
  }

  @Test
  public void testSerializationFormatForBackwardsCompatibility() {
    KafkaChangelogSSPOffset kafkaChangelogSSPOffset = new KafkaChangelogSSPOffset(CheckpointId.create(), "offset");

    // WARNING: This format is written to persisted remotes stores and local files, making a change in the format
    // would be backwards incompatible
    String expectedSerializationFormat = kafkaChangelogSSPOffset.getCheckpointId() + KafkaChangelogSSPOffset.SEPARATOR +
        kafkaChangelogSSPOffset.getChangelogOffset();
    assertEquals(expectedSerializationFormat, kafkaChangelogSSPOffset.toString());
    assertEquals(kafkaChangelogSSPOffset, KafkaChangelogSSPOffset.fromString(expectedSerializationFormat));
  }

  @Test
  public void testNullSerializationFormatForBackwardsCompatibility() {
    KafkaChangelogSSPOffset kafkaChangelogSSPOffset = new KafkaChangelogSSPOffset(CheckpointId.create(), null);

    // WARNING: This format is written to persisted remotes stores and local files, making a change in the format
    // would be backwards incompatible
    String expectedSerializationFormat = kafkaChangelogSSPOffset.getCheckpointId() + KafkaChangelogSSPOffset.SEPARATOR +
        "null";
    assertEquals(expectedSerializationFormat, kafkaChangelogSSPOffset.toString());
    assertEquals(kafkaChangelogSSPOffset, KafkaChangelogSSPOffset.fromString(expectedSerializationFormat));
  }
}
