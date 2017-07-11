package org.apache.samza.operators;

import org.apache.samza.serializers.Serde;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.system.StreamSpec;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by yipan on 6/11/17.
 */
public class StreamDescriptor {

  private static class StreamIO<K, V> {
    private final String streamId;
    private final Serde<K> keySerde;
    private final Serde<V> msgSerde;
    private final IOSystem system;

    private StreamIO(String streamId, IOSystem system, Serde<K> keySerde, Serde<V> msgSerde) {
      this.streamId = streamId;
      this.system = system;
      this.keySerde = keySerde;
      this.msgSerde = msgSerde;
    }

    private Builder toBuilder() {
      return new Builder(this);
    }

    class Builder {
      private String streamId;
      private Serde<K> keySerde;
      private Serde<V> msgSerde;
      private IOSystem system;

      Builder(StreamIO<K, V> kvstream) {
        this.streamId = kvstream.streamId;
        this.keySerde = kvstream.keySerde;
        this.msgSerde = kvstream.msgSerde;
        this.system = kvstream.system;
      }

      Builder setKeySerde(Serde<K> serde) {
        this.keySerde = serde;
        return this;
      }

      Builder setMsgSerde(Serde<V> serde) {
        this.msgSerde = serde;
        return this;
      }

      Builder setSystem(IOSystem system) {
        this.system = system;
        return this;
      }

      StreamIO<K, V> build() {
        return new StreamIO<K, V>(this.streamId, this.system, this.keySerde, this.msgSerde);
      }
    }
  }

  public static class Input<K, V> {
    private final StreamIO<K, V> iostream;

    private Input(StreamIO<K, V> kvstream) {
      this.iostream = kvstream;
    }

    public String getStreamId() {
      return this.iostream.streamId;
    }

    public StreamSpec getStreamSpec() {
      // generate {@link StreamSpec}
      return null;
    }

    public Input<K, V> withKeySerde(Serde<K> keySerde) {
      return new Input<K, V>(this.iostream.toBuilder().setKeySerde(keySerde).build());
    }

    public Input<K, V> withMsgSerde(Serde<V> msgSerde) {
      return new Input<K,V>(this.iostream.toBuilder().setMsgSerde(msgSerde).build());
    }

    public Input<K, V> from(IOSystem system) {
      return new Input<K, V>(this.iostream.toBuilder().setSystem(system).build());
    }

  }

  public static class Output<K, V> {
    private final StreamIO<K, V> iostream;

    private Output(StreamIO<K, V> kvstream) {
      this.iostream = kvstream;
    }

    public String getStreamId() {
      return this.iostream.streamId;
    }

    public StreamSpec getStreamSpec() {
      // generate {@link StreamSpec}
      return null;
    }

    public Output<K, V> withKeySerde(Serde<K> keySerde) {
      return new Output<K, V>(this.iostream.toBuilder().setKeySerde(keySerde).build());
    }

    public Output<K, V> withMsgSerde(Serde<V> msgSerde) {
      return new Output<K,V>(this.iostream.toBuilder().setMsgSerde(msgSerde).build());
    }

    public Output<K, V> from(IOSystem system) {
      return new Output<K, V>(this.iostream.toBuilder().setSystem(system).build());
    }

  }

  public static <K, V> StreamDescriptor.Input<K, V> input(String strmId) {
    return new Input(new StreamIO<K, V>(strmId, null, null, null));
  }

  public static <K, V> StreamDescriptor.Output<K, V> output(String strmId) {
    return new Output(new StreamIO<K, V>(strmId, null, null, null));
  }
}
