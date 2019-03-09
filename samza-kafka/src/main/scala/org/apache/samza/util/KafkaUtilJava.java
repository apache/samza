package org.apache.samza.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class KafkaUtilJava {
  public static int getRecordSize(ConsumerRecord<?, ?> consumerRecord) {
    int keySize = (consumerRecord.key() == null) ? 0 : consumerRecord.serializedKeySize();
    int valueSize = (consumerRecord.value() == null) ? 0 : consumerRecord.serializedValueSize();
    return keySize + valueSize;
  }
}
