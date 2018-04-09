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
package org.apache.samza.operators.spec;

import java.io.Serializable;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;


public class OutputStreamImpl<M> implements OutputStream<M>, Serializable {

  private final SystemStream systemStream;
  private final boolean isKeyed;

  // Avoid making per-task copies of StreamSpec/Serde, since the following transient
  // members are only used to generate configuration
  private transient final StreamSpec streamSpec;
  private transient final Serde keySerde;
  private transient final Serde valueSerde;

  public OutputStreamImpl(StreamSpec streamSpec,
      Serde keySerde, Serde valueSerde, boolean isKeyed) {
    this.streamSpec = streamSpec;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.systemStream = streamSpec.toSystemStream();
    this.isKeyed = isKeyed;
  }

  public StreamSpec getStreamSpec() {
    return streamSpec;
  }

  public Serde getKeySerde() {
    return keySerde;
  }

  public Serde getValueSerde() {
    return valueSerde;
  }

  public SystemStream getSystemStream() {
    return this.systemStream;
  }

  public boolean isKeyed() {
    return isKeyed;
  }
}
