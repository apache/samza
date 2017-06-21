package org.apache.samza.operators;

import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;

/**
 * Created by yipan on 6/11/17.
 */
public class StreamDescriptor<K, V> {
  private IOSystem inputSystem;
  private Serde<K> keySerde;
  private Serde<V> msgSerde;
  private String streamId;

  public StreamDescriptor<K, V> withKeySerde(Serde<K> keySerde) {
    this.keySerde = keySerde;
    return this;
  }

  public StreamDescriptor<K, V> withMsgSerde(Serde<V> msgSerde) {
    this.msgSerde = msgSerde;
    return this;
  }

  public StreamDescriptor<K, V> from(IOSystem system) {
    this.inputSystem = system;
    return this;
  }

  public static <K, V> StreamDescriptor<K, V> create(String streamId) {
    return new StreamDescriptor<K, V>(streamId);
  }

  private StreamDescriptor(String streamId) {
    this.streamId = streamId;
  }

  public String getStreamId() {
    return this.streamId;
  }

}
