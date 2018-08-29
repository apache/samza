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

public class OutputStreamImpl<M> implements OutputStream<M>, Serializable {

  private final String streamId;
  private final boolean isKeyed;

  /**
   * The following fields are serialized by the ExecutionPlanner when generating the configs for the output stream, and
   * deserialized once during startup in SamzaContainer. They don't need to be deserialized here on a per-task basis
   *
   * Serdes are optional for intermediate streams and may be specified for job.default.system in configuration instead.
   */
  private transient final Serde keySerde;
  private transient final Serde valueSerde;

  public OutputStreamImpl(String streamId, Serde keySerde, Serde valueSerde, boolean isKeyed) {
    this.streamId = streamId;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.isKeyed = isKeyed;
  }

  public String getStreamId() {
    return streamId;
  }

  /**
   * Get the key serde for this output stream if any.
   *
   * @return the key serde if any, else null
   */
  public Serde getKeySerde() {
    return keySerde;
  }

  /**
   * Get the value serde for this output stream if any.
   *
   * @return the value serde if any, else null
   */
  public Serde getValueSerde() {
    return valueSerde;
  }

  public boolean isKeyed() {
    return isKeyed;
  }
}
