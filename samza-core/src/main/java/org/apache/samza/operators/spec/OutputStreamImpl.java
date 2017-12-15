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


public class OutputStreamImpl<M> implements OutputStream<M>, Serializable {

  private final StreamSpec streamSpec;
  private final Serde keySerde;
  private final Serde valueSerde;
  private final boolean isKeyedOutput;

  public OutputStreamImpl(StreamSpec streamSpec,
      Serde keySerde, Serde valueSerde, boolean isKeyedOutput) {
    this.streamSpec = streamSpec;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.isKeyedOutput = isKeyedOutput;
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

  public boolean isKeyedOutput() {
    return isKeyedOutput;
  }

}
