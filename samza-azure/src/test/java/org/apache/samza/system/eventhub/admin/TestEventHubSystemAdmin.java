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

import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.eventhub.EventHubSystemFactory;
import org.apache.samza.system.eventhub.MockEventHubConfigFactory;
import org.apache.samza.system.eventhub.consumer.EventHubSystemConsumer;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

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
    Assert.assertNull(eventHubSystemAdmin.offsetComparator("100", EventHubSystemConsumer.END_OF_STREAM));
    Assert.assertNull(eventHubSystemAdmin.offsetComparator(EventHubSystemConsumer.END_OF_STREAM, EventHubSystemConsumer.END_OF_STREAM));
  }

  @Ignore("Integration Test")
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
          String expectedUpcomingOffset = String.valueOf(Long.parseLong(metadata.getNewestOffset()) + 1);
          Assert.assertEquals(expectedUpcomingOffset, metadata.getUpcomingOffset());
        });
    }
  }

}
