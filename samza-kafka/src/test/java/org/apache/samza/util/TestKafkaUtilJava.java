package org.apache.samza.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestKafkaUtilJava {
  @Test
  public void testGetRecordSize() {
    String key = "key";
    String value = "value";
    ConsumerRecord<String, String> consumerRecord = mock(ConsumerRecord.class);

    // null key and value
    when(consumerRecord.key()).thenReturn(null);
    when(consumerRecord.value()).thenReturn(null);
    assertEquals(0, KafkaUtilJava.getRecordSize(consumerRecord));

    // null key, non-null value
    when(consumerRecord.key()).thenReturn(null);
    when(consumerRecord.value()).thenReturn(value);
    when(consumerRecord.serializedValueSize()).thenReturn(value.length());
    assertEquals(value.length(), KafkaUtilJava.getRecordSize(consumerRecord));

    // non-null key, null value (this case seems odd, but the ConsumerRecord API seems to allow it)
    when(consumerRecord.key()).thenReturn(key);
    when(consumerRecord.serializedKeySize()).thenReturn(key.length());
    when(consumerRecord.value()).thenReturn(null);
    assertEquals(key.length(), KafkaUtilJava.getRecordSize(consumerRecord));

    // non-null key, non-null value
    when(consumerRecord.key()).thenReturn(key);
    when(consumerRecord.serializedKeySize()).thenReturn(key.length());
    when(consumerRecord.value()).thenReturn(value);
    when(consumerRecord.serializedValueSize()).thenReturn(value.length());
    assertEquals(key.length() + value.length(), KafkaUtilJava.getRecordSize(consumerRecord));
  }
}