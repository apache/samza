package org.apache.samza.system.standalone;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.ZkConfig;


public class ZkTestUtils {
  private static final String ZK_JOB_COORDINATOR_FACTORY = "org.apache.samza.zk.ZkJobCoordinatorFactory";
  private static final String KAFKA_SYSTEM_FACTORY = "org.apache.samza.system.kafka.KafkaSystemFactory";

  public static final String JOB_NAME = "job.name";
  private static final String TASK_CLASS = "task.class";
  private static final String JOB_COORDINATOR_FACTORY = "job.coordinator.factory";

  private static final String ZOOKEEPER_CONNECT_FORMAT_STRING = "systems.%s.consumer.zookeeper.connect";
  private static final String BOOTSTRAP_SERVERS_FORMAT_STRING = "systems.%s.producer.bootstrap.servers";
  private static final String SERIALIZERS_REGISTRY_FORMAT_STRING = "serializers.registry.%s.class";
  private static final String SYSTEM_FACTORY_FORMAT_STRING = "systems.%s.samza.factory";
  private static final String SYSTEM_OFFSET_DEFAULT_FORMAT_STRING = "systems.%s.samza.offset.default";
  private static final String OFFSET_OLDEST = "oldest";

  public enum SerdeAlias {
    STRING,
    INT
  }
  private ZkTestUtils() {

  }

  public static Map<String, String> getZkConfigs(String jobName, String taskClass, String zkConnect) {
    return new HashMap<String, String>() {
      {
        put(JOB_NAME, jobName);
        put(TASK_CLASS, taskClass);
        put(JOB_COORDINATOR_FACTORY, ZK_JOB_COORDINATOR_FACTORY);
        put(ZkConfig.ZK_CONNECT, zkConnect);
      }
    };
  }

  private static Map<String, String> getSerdeConfigs(String systemName, String keyOrMsg, SerdeAlias serde) {
    Map<String, String> result = new HashMap<>();
    switch (serde) {
      case STRING:
        result.put(
            String.format(SERIALIZERS_REGISTRY_FORMAT_STRING, "string"),
            "org.apache.samza.serializers.StringSerdeFactory");
        result.put(
            String.format("systems.%s.samza.%s.serde", systemName, keyOrMsg),
            "string");
        break;
      case INT:
        result.put(
            String.format(SERIALIZERS_REGISTRY_FORMAT_STRING, "int"),
            "org.apache.samza.serializers.IntegerSerdeFactory");
        result.put(
            String.format("systems.%s.samza.%s.serde", systemName, keyOrMsg),
            "int");
        break;
    }
    return result;
  }

  public static Map<String, String> getKafkaSystemConfigs(
      String systemName,
      String bootstrapServers,
      String zkConnection,
      SerdeAlias keySerde,
      SerdeAlias msgSerde,
      boolean resetOffsetToEarliest) {
    Map<String, String> result = new HashMap<>();
    result.put(String.format(SYSTEM_FACTORY_FORMAT_STRING, systemName), KAFKA_SYSTEM_FACTORY);
    result.put(String.format(BOOTSTRAP_SERVERS_FORMAT_STRING, systemName), bootstrapServers);
    result.put(String.format(ZOOKEEPER_CONNECT_FORMAT_STRING, systemName), zkConnection);
    if (keySerde != null) {
      result.putAll(getSerdeConfigs(systemName, "key", keySerde));
    }
    if (msgSerde != null) {
      result.putAll(getSerdeConfigs(systemName, "msg", msgSerde));
    }
    if (resetOffsetToEarliest) {
      result.put(String.format(SYSTEM_OFFSET_DEFAULT_FORMAT_STRING, systemName), OFFSET_OLDEST);
    }
    return result;
  }

}
