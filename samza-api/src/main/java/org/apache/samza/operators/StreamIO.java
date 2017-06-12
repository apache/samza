package org.apache.samza.operators;

import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;

/**
 * Created by yipan on 6/11/17.
 */
public class StreamIO {

  public static <K, V> Input<K, V> read(String streamId) {
    return new Input<K, V>();
  }

  public static <K, V> Output<K, V> write(String streamId) {
    return new Output<K, V>();
  }

  public static class Input<K, V> extends StreamSpec {
    private IOSystem inputSystem;
    private Serde<K> keySerde;
    private Serde<V> msgSerde;

    public Input<K, V> withKeySerde(Serde<K> keySerde) {
      this.keySerde = keySerde;
      return this;
    }

    public Input<K, V> withMsgSerde(Serde<V> msgSerde) {
      this.msgSerde = msgSerde;
      return this;
    }

    public Input from(IOSystem system) {
      this.inputSystem = system;
      return this;
    }
  }

  public static class Output<K, V> extends StreamSpec {
    private IOSystem outputSystem;
    private Serde<K> keySerde;
    private Serde<V> msgSerde;

    public Output<K, V> withKeySerde(Serde<K> keySerde) {
      this.keySerde = keySerde;
      return this;
    }

    public Output<K, V> withMsgSerde(Serde<V> msgSerde) {
      this.msgSerde = msgSerde;
      return this;
    }

    public Output to(IOSystem system) {
      this.outputSystem = system;
      return this;
    }
  }
}
