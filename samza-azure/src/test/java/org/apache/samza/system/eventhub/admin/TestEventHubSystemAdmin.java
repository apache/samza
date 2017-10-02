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

package org.apache.samza.system.eventhub.admin;

import junit.framework.Assert;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.EventHubSystemFactory;
import org.apache.samza.system.eventhub.MockEventHubConfigFactory;
import org.apache.samza.system.eventhub.consumer.EventHubSystemConsumer;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.samza.system.eventhub.MockEventHubConfigFactory.*;

public class TestEventHubSystemAdmin {

  @Test
  public void testOffsetComparison() {
    EventHubSystemFactory eventHubSystemFactory = new EventHubSystemFactory();
    EventHubSystemAdmin eventHubSystemAdmin = (EventHubSystemAdmin) eventHubSystemFactory.getAdmin(SYSTEM_NAME,
            MockEventHubConfigFactory.getEventHubConfig(EventHubSystemProducer.PartitioningMethod.EVENT_HUB_HASHING));
    Assert.assertEquals(-1, eventHubSystemAdmin.offsetComparator("100", "200").intValue());
    Assert.assertEquals(0, eventHubSystemAdmin.offsetComparator("150", "150").intValue());
    Assert.assertEquals(1, eventHubSystemAdmin.offsetComparator("200", "100").intValue());
    Assert.assertNull(eventHubSystemAdmin.offsetComparator("1", "a"));
    Assert.assertEquals(-1, eventHubSystemAdmin
            .offsetComparator("100", EventHubSystemConsumer.END_OF_STREAM).intValue());
    Assert.assertEquals(0, eventHubSystemAdmin.offsetComparator(EventHubSystemConsumer.END_OF_STREAM,
            EventHubSystemConsumer.END_OF_STREAM).intValue());
    Assert.assertEquals(1, eventHubSystemAdmin
            .offsetComparator(EventHubSystemConsumer.END_OF_STREAM, "100").intValue());
    Assert.assertEquals(-1, eventHubSystemAdmin
            .offsetComparator(EventHubSystemConsumer.START_OF_STREAM, "10").intValue());
  }

  @Test
  public void testGetNextOffset() {
    EventHubSystemFactory eventHubSystemFactory = new EventHubSystemFactory();
    SystemAdmin eventHubSystemAdmin = eventHubSystemFactory.getAdmin(SYSTEM_NAME,
            MockEventHubConfigFactory.getEventHubConfig(EventHubSystemProducer.PartitioningMethod.EVENT_HUB_HASHING));
    Map<SystemStreamPartition, String> offsets = new HashMap<>();
    SystemStreamPartition ssp0 = new SystemStreamPartition(SYSTEM_NAME, STREAM_NAME1, new Partition(0));
    SystemStreamPartition ssp1 = new SystemStreamPartition(SYSTEM_NAME, STREAM_NAME1, new Partition(1));
    SystemStreamPartition ssp2 = new SystemStreamPartition(SYSTEM_NAME, STREAM_NAME1, new Partition(2));
    offsets.put(ssp0, Integer.toString(0));
    offsets.put(ssp1, EventHubSystemConsumer.END_OF_STREAM);
    offsets.put(ssp2, EventHubSystemConsumer.START_OF_STREAM);

    Map<SystemStreamPartition, String> updatedOffsets = eventHubSystemAdmin.getOffsetsAfter(offsets);
    Assert.assertEquals(offsets.size(), updatedOffsets.size());
    Assert.assertEquals("1", updatedOffsets.get(ssp0));
    Assert.assertEquals("-2", updatedOffsets.get(ssp1));
    Assert.assertEquals("0", updatedOffsets.get(ssp2));
  }

  @Test
  public void testGetStreamMetadata() {
    EventHubSystemFactory eventHubSystemFactory = new EventHubSystemFactory();
    SystemAdmin eventHubSystemAdmin = eventHubSystemFactory.getAdmin(SYSTEM_NAME,
            MockEventHubConfigFactory.getEventHubConfig(EventHubSystemProducer.PartitioningMethod.EVENT_HUB_HASHING));
    Set<String> streams = new HashSet<>();
    streams.add(STREAM_NAME1);
    streams.add(STREAM_NAME2);
    Map<String, SystemStreamMetadata> metadataMap = eventHubSystemAdmin.getSystemStreamMetadata(streams);

    for (String stream : streams) {
      Assert.assertTrue(metadataMap.containsKey(stream));
      Assert.assertEquals(stream, metadataMap.get(stream).getStreamName());
      Assert.assertNotNull(metadataMap.get(stream).getSystemStreamPartitionMetadata());

      Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadataMap =
              metadataMap.get(stream).getSystemStreamPartitionMetadata();
      Assert.assertTrue(partitionMetadataMap.size() >= MIN_EVENTHUB_ENTITY_PARTITION);
      Assert.assertTrue(partitionMetadataMap.size() <= MAX_EVENTHUB_ENTITY_PARTITION);
      partitionMetadataMap.forEach((partition, metadata) -> {
          Assert.assertEquals(EventHubSystemConsumer.START_OF_STREAM, metadata.getOldestOffset());
          Assert.assertNotSame(EventHubSystemConsumer.END_OF_STREAM, metadata.getNewestOffset());
          Assert.assertTrue(Long.parseLong(EventHubSystemConsumer.END_OF_STREAM)
                  <= Long.parseLong(metadata.getNewestOffset()));
          Assert.assertEquals(EventHubSystemConsumer.END_OF_STREAM, metadata.getUpcomingOffset());
        });
    }
  }

}
