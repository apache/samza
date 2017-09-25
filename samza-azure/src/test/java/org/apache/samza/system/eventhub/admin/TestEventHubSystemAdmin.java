package org.apache.samza.system.eventhub.admin;

import junit.framework.Assert;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.eventhub.EventHubClientWrapper;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.EventHubSystemFactory;
import org.apache.samza.system.eventhub.consumer.EventHubSystemConsumer;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;
import org.junit.Test;

import java.util.HashMap;

public class TestEventHubSystemAdmin {

  public static final String SYSTEM_NAME = "eventhub-s1";
  public static final String STREAM_NAME1 = "test_stream1";

  public static final String EVENTHUB_NAMESPACE = "";
  public static final String EVENTHUB_ENTITY1 = "";
  public static final String EVENTHUB_KEY_NAME = "";
  public static final String EVENTHUB_KEY = "";

  private Config createEventHubConfig(EventHubClientWrapper.PartitioningMethod partitioningMethod) {
    HashMap<String, String> mapConfig = new HashMap<>();
    mapConfig.put(EventHubSystemProducer.CONFIG_PARTITIONING_METHOD, partitioningMethod.toString());
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, SYSTEM_NAME), STREAM_NAME1);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_NAMESPACE);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_ENTITY1);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_KEY_NAME);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_KEY);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_CONSUMER_START_POSITION, SYSTEM_NAME, STREAM_NAME1), "earliest");
    return new MapConfig(mapConfig);
  }

  @Test
  public void testOffsetComparison() {
    EventHubSystemFactory eventHubSystemFactory = new EventHubSystemFactory();
    EventHubSystemAdmin eventHubSystemAdmin = (EventHubSystemAdmin) eventHubSystemFactory.getAdmin(SYSTEM_NAME,
            createEventHubConfig(EventHubClientWrapper.PartitioningMethod.EVENT_HUB_HASHING));
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
    EventHubSystemAdmin eventHubSystemAdmin = (EventHubSystemAdmin) eventHubSystemFactory.getAdmin(SYSTEM_NAME,
            createEventHubConfig(EventHubClientWrapper.PartitioningMethod.EVENT_HUB_HASHING));

  }

}
