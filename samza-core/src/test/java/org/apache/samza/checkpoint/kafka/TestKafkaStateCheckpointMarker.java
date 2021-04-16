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

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;
import scala.Option;

import static org.apache.samza.checkpoint.kafka.KafkaStateCheckpointMarker.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class TestKafkaStateCheckpointMarker {
  @Test
  public void testSerializeDeserialize() {
    SystemStreamPartition ssp = new SystemStreamPartition("system", "stream", new Partition(1));
    KafkaStateCheckpointMarker marker = new KafkaStateCheckpointMarker(ssp, "offset");
    KafkaStateCheckpointMarker deserializedMarker = KafkaStateCheckpointMarker.fromString(marker.toString());

    assertEquals(marker.getChangelogOffset(), deserializedMarker.getChangelogOffset());
    assertEquals(marker.getChangelogSSP(), deserializedMarker.getChangelogSSP());
    assertEquals(marker, deserializedMarker);
  }

  @Test
  public void testSerializeDeserializeNullOffsets() {
    SystemStreamPartition ssp = new SystemStreamPartition("system", "stream", new Partition(1));
    KafkaStateCheckpointMarker marker = new KafkaStateCheckpointMarker(ssp, null);
    KafkaStateCheckpointMarker deserializedMarker = KafkaStateCheckpointMarker.fromString(marker.toString());

    assertNull(deserializedMarker.getChangelogOffset());
    assertEquals(marker.getChangelogSSP(), deserializedMarker.getChangelogSSP());
    assertEquals(marker, deserializedMarker);
  }

  @Test
  public void testSerdeFormatForBackwardsCompatibility() {
    SystemStreamPartition ssp = new SystemStreamPartition("system", "stream", new Partition(1));
    KafkaStateCheckpointMarker marker = new KafkaStateCheckpointMarker(ssp, "offset");
    String expectedSerializedMarker = String.format("%s%s%s%s%s%s%s",
        ssp.getSystem(), SEPARATOR, ssp.getStream(), SEPARATOR,
        ssp.getPartition().getPartitionId(), SEPARATOR, "offset");

    assertEquals(expectedSerializedMarker, marker.toString());
    assertEquals(marker, KafkaStateCheckpointMarker.fromString(expectedSerializedMarker));
  }

  @Test
  public void testNullOffsetSerdeFormatForBackwardsCompatibility() {
    SystemStreamPartition ssp = new SystemStreamPartition("system", "stream", new Partition(1));
    KafkaStateCheckpointMarker marker = new KafkaStateCheckpointMarker(ssp, null);
    String expectedSerializedMarker = String.format("%s%s%s%s%s%s%s",
        ssp.getSystem(), SEPARATOR, ssp.getStream(), SEPARATOR,
        ssp.getPartition().getPartitionId(), SEPARATOR, "null");

    assertEquals(expectedSerializedMarker, marker.toString());
    assertEquals(marker, KafkaStateCheckpointMarker.fromString(expectedSerializedMarker));
    assertNull(KafkaStateCheckpointMarker.fromString(expectedSerializedMarker).getChangelogOffset());
  }

  @Test
  public void testStateCheckpointMarkerToSSPOffsetMap() {
    SystemStreamPartition ssp1 = new SystemStreamPartition("system1", "stream1", new Partition(1));
    KafkaStateCheckpointMarker marker1 = new KafkaStateCheckpointMarker(ssp1, "offset1");
    SystemStreamPartition ssp2 = new SystemStreamPartition("system2", "stream2", new Partition(2));
    KafkaStateCheckpointMarker marker2 = new KafkaStateCheckpointMarker(ssp2, null);
    Map<String, String> storesToKSCM = ImmutableMap.of(
        "store1", marker1.toString(),
        "store2", marker2.toString()
    );
    Map<String, Map<String, String>> factoryToSCMs = ImmutableMap.of(
        KAFKA_STATE_BACKEND_FACTORY_NAME, storesToKSCM,
        "factory2", Collections.EMPTY_MAP // factory2 should be ignored
    );

    Map<SystemStreamPartition, Option<String>> sspToOffsetOption = KafkaStateCheckpointMarker
        .scmsToSSPOffsetMap(factoryToSCMs);

    assertEquals(2, sspToOffsetOption.size());
    assertTrue(sspToOffsetOption.containsKey(ssp1));
    assertEquals(sspToOffsetOption.get(ssp1).get(), marker1.getChangelogOffset());
    assertEquals(ssp1, marker1.getChangelogSSP());
    assertTrue(sspToOffsetOption.containsKey(ssp2));
    assertTrue(sspToOffsetOption.get(ssp2).isEmpty());
  }

  @Test
  public void testStateCheckpointMarkerToSSPOffsetMapNoFactoryFound() {
    Map<String, Map<String, String>> factoryToSCMs = ImmutableMap.of(
        "factory1", Collections.EMPTY_MAP, // factory1 should be ignored
        "factory2", Collections.EMPTY_MAP  // factory2 should be ignored
    );

    Map<SystemStreamPartition, Option<String>> sspToOffsetOption = KafkaStateCheckpointMarker
        .scmsToSSPOffsetMap(factoryToSCMs);

    assertEquals(0, sspToOffsetOption.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStateCheckpointMarkerToSSPOffsetMapDeserializationError() {
    Map<String, String> storesToSCM = ImmutableMap.of(
        "store1", "blobId-1234"
    );
    Map<String, Map<String, String>> factoryToSCMs = ImmutableMap.of(
        "factory2", Collections.EMPTY_MAP, // factory2 should be ignored
        KAFKA_STATE_BACKEND_FACTORY_NAME, storesToSCM
    );

    KafkaStateCheckpointMarker.scmsToSSPOffsetMap(factoryToSCMs);
  }
}
