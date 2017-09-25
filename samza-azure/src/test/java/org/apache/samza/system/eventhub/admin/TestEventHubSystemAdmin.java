package org.apache.samza.system.eventhub.admin;

import junit.framework.Assert;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.EventHubClientWrapper;
import org.apache.samza.system.eventhub.EventHubSystemFactory;
import org.apache.samza.system.eventhub.MockConfigFactory;
import org.apache.samza.system.eventhub.consumer.EventHubSystemConsumer;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.samza.system.eventhub.MockConfigFactory.*;

public class TestEventHubSystemAdmin {

  @Test
  public void testOffsetComparison() {
    EventHubSystemFactory eventHubSystemFactory = new EventHubSystemFactory();
    EventHubSystemAdmin eventHubSystemAdmin = (EventHubSystemAdmin) eventHubSystemFactory.getAdmin(SYSTEM_NAME,
            MockConfigFactory.getEventHubConfig(EventHubClientWrapper.PartitioningMethod.EVENT_HUB_HASHING));
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
            MockConfigFactory.getEventHubConfig(EventHubClientWrapper.PartitioningMethod.EVENT_HUB_HASHING));
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
            MockConfigFactory.getEventHubConfig(EventHubClientWrapper.PartitioningMethod.EVENT_HUB_HASHING));
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
      partitionMetadataMap.forEach((partition, metadata) -> {
        Assert.assertEquals(EventHubSystemConsumer.START_OF_STREAM, metadata.getOldestOffset());
        Assert.assertEquals(EventHubSystemConsumer.END_OF_STREAM, metadata.getNewestOffset());
        Assert.assertEquals(EventHubSystemConsumer.END_OF_STREAM, metadata.getUpcomingOffset());
      });
    }
  }

}
