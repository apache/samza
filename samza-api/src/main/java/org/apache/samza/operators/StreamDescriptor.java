/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.operators;

import java.io.Serializable;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;

public class StreamDescriptor {

  private static class StreamIO<K, V> implements Serializable {
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

    public String getStreamId() {
      return this.streamId;
    }

    public Serde<K> getKeySerde() {
      return this.keySerde;
    }

    public Serde<V> getMsgSerde() {
      return this.msgSerde;
    }

    public StreamSpec getStreamSpec() {
      return new StreamSpec(this.streamId, null, system.getSystemName());
    }

    IOSystem getIOSystem() {
      return this.system;
    }
  }

  public static class Input<K, V> extends StreamIO<K, V> implements Serializable {

    private Input(String streamId, IOSystem system, Serde<K> keySerde, Serde<V> msgSerde) {
      super(streamId, system, keySerde, msgSerde);
    }

    public Input<K, V> withKeySerde(Serde<K> keySerde) {
      return this.toBuilder().setKeySerde(keySerde).build();
    }

    public Input<K, V> withMsgSerde(Serde<V> msgSerde) {
      return this.toBuilder().setMsgSerde(msgSerde).build();
    }

    public Input<K, V> from(IOSystem system) {
      return this.toBuilder().setSystem(system).build();
    }

    Builder toBuilder() {
      return new Builder(this);
    }

    class Builder {
      private String streamId;
      private Serde<K> keySerde;
      private Serde<V> msgSerde;
      private IOSystem system;

      Builder(Input<K, V> kvstream) {
        this.streamId = kvstream.getStreamId();
        this.keySerde = kvstream.getKeySerde();
        this.msgSerde = kvstream.getMsgSerde();
        this.system = kvstream.getIOSystem();
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

      Input<K, V> build() {
        return new Input<K, V>(this.streamId, this.system, this.keySerde, this.msgSerde);
      }
    }
  }

  public static class Output<K, V> extends StreamIO<K, V> implements Serializable {

    private Output() {
      super(null, null, null, null);
    }

    private Output(String streamId, IOSystem system, Serde<K> keySerde, Serde<V> msgSerde) {
      super(streamId, system, keySerde, msgSerde);
    }

    public Output<K, V> withKeySerde(Serde<K> keySerde) {
      return this.toBuilder().setKeySerde(keySerde).build();
    }

    public Output<K, V> withMsgSerde(Serde<V> msgSerde) {
      return this.toBuilder().setMsgSerde(msgSerde).build();
    }

    public Output<K, V> from(IOSystem system) {
      return this.toBuilder().setSystem(system).build();
    }

    Builder toBuilder() {
      return new Builder(this);
    }

    class Builder {
      private String streamId;
      private Serde<K> keySerde;
      private Serde<V> msgSerde;
      private IOSystem system;

      Builder(Output<K, V> kvstream) {
        this.streamId = kvstream.getStreamId();
        this.keySerde = kvstream.getKeySerde();
        this.msgSerde = kvstream.getMsgSerde();
        this.system = kvstream.getIOSystem();
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

      Output<K, V> build() {
        return new Output<K, V>(this.streamId, this.system, this.keySerde, this.msgSerde);
      }
    }
  }

  public static <K, V> StreamDescriptor.Input<K, V> input(String strmId) {
    return new Input<>(strmId, null, null, null);
  }

  public static <K, V> StreamDescriptor.Output<K, V> output(String strmId) {
    return new Output<>(strmId, null, null, null);
  }
}
