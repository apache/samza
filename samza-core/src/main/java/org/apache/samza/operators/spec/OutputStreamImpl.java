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
import java.util.Optional;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.serializers.Serde;

public class OutputStreamImpl<M> implements OutputStream<M>, Serializable {

  private final String streamId;
  private final boolean isKeyed;

  /**
   * The following fields are serialized by the ExecutionPlanner when generating the configs for the output stream, and
   * deserialized once during startup in SamzaContainer. They don't need to be deserialized here on a per-task basis
   *
   * Serdes are optional for intermediate streams and may be specified for job.default.system in configuration instead.
   */
  private transient final Optional<Serde> keySerdeOptional;
  private transient final Optional<Serde> valueSerdeOptional;

  public OutputStreamImpl(String streamId, Serde keySerde, Serde valueSerde, boolean isKeyed) {
    this.streamId = streamId;
    this.keySerdeOptional = Optional.ofNullable(keySerde);
    this.valueSerdeOptional = Optional.ofNullable(valueSerde);
    this.isKeyed = isKeyed;
  }

  public String getStreamId() {
    return streamId;
  }

  public Optional<Serde> getKeySerde() {
    return keySerdeOptional;
  }

  public Optional<Serde> getValueSerde() {
    return valueSerdeOptional;
  }

  public boolean isKeyed() {
    return isKeyed;
  }
}
