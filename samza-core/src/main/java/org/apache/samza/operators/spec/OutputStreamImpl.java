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

import org.apache.samza.operators.OutputStream;
import org.apache.samza.system.StreamSpec;

import java.util.function.Function;

public class OutputStreamImpl<K, V, M> implements OutputStream<K, V, M> {

  private final StreamSpec streamSpec;
  private final Function<M, K> keyExtractor;
  private final Function<M, V> msgExtractor;

  public OutputStreamImpl(StreamSpec streamSpec,
      Function<M, K> keyExtractor, Function<M, V> msgExtractor) {
    this.streamSpec = streamSpec;
    this.keyExtractor = keyExtractor;
    this.msgExtractor = msgExtractor;
  }

  public StreamSpec getStreamSpec() {
    return streamSpec;
  }

  public Function<M, K> getKeyExtractor() {
    return keyExtractor;
  }

  public Function<M, V> getMsgExtractor() {
    return msgExtractor;
  }
}
