package org.apache.samza.operators;

import org.apache.samza.config.Config;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yipan on 6/11/17.
 */
public class KafkaSystem implements IOSystem {
  private static Map<String, KafkaSystem> kafkaSystems = new HashMap<>();

  public static KafkaSystem create(String systemId) {
    if (kafkaSystems.containsKey(systemId)) {
      return kafkaSystems.get(systemId);
    }
    KafkaSystem system = new KafkaSystem();
    kafkaSystems.putIfAbsent(systemId, system);
    return kafkaSystems.get(systemId);
  }

  public KafkaSystem withBootstrapServers(String brokerList) {
    return this;
  }

  public KafkaSystem withConsumerProperties(Config consumerConfig) {
    return this;
  }

  public KafkaSystem withProducerProperties(Config producerConfig) {
    return this;
  }
}
