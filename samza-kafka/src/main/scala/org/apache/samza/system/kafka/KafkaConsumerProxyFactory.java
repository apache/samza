package org.apache.samza.system.kafka;

/**
 * Used to create an instance of {@link KafkaConsumerProxy}. This can be overridden in case an extension of
 * {@link KafkaConsumerProxy} needs to be used within kafka system components like {@link KafkaSystemConsumer}.
 */
public interface KafkaConsumerProxyFactory<K, V> {
  KafkaConsumerProxy<K, V> create(KafkaSystemConsumer<K, V>.KafkaConsumerMessageSink messageSink);
}
