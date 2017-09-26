package org.apache.samza.system.eventhub;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;

import java.util.HashMap;

public class MockEventHubConfigFactory {

  public static final String SYSTEM_NAME = "eventhub-s1";
  public static final String STREAM_NAME1 = "test_stream1";
  public static final String STREAM_NAME2 = "test_stream2";

  // Add target Event Hub connection information here
  public static final String EVENTHUB_NAMESPACE = "";
  public static final String EVENTHUB_KEY_NAME = "";
  public static final String EVENTHUB_KEY = "";
  public static final String EVENTHUB_ENTITY1 = "";
  public static final String EVENTHUB_ENTITY2 = "";

  public static final int MIN_EVENTHUB_ENTITY_PARTITION = 2;
  public static final int MAX_EVENTHUB_ENTITY_PARTITION = 32;

  public static Config getEventHubConfig(EventHubClientWrapper.PartitioningMethod partitioningMethod) {
    HashMap<String, String> mapConfig = new HashMap<>();
    mapConfig.put(String.format(EventHubConfig.CONFIG_PRODUCER_PARTITION_METHOD, SYSTEM_NAME), partitioningMethod.toString());
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, SYSTEM_NAME), STREAM_NAME1);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_NAMESPACE);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_ENTITY1);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_KEY_NAME);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_KEY);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_CONSUMER_START_POSITION, SYSTEM_NAME, STREAM_NAME1), "earliest");

    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, SYSTEM_NAME), STREAM_NAME2);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, SYSTEM_NAME, STREAM_NAME2), EVENTHUB_NAMESPACE);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, SYSTEM_NAME, STREAM_NAME2), EVENTHUB_ENTITY2);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, SYSTEM_NAME, STREAM_NAME2), EVENTHUB_KEY_NAME);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, SYSTEM_NAME, STREAM_NAME2), EVENTHUB_KEY);
    mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_CONSUMER_START_POSITION, SYSTEM_NAME, STREAM_NAME2), "earliest");

    return new MapConfig(mapConfig);
  }
}
