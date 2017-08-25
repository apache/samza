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
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.system.StreamSpec;

public class OutputStreamImpl<K, V, M> implements OutputStream<K, V, M>, Serializable {

  private final StreamDescriptor.Output<K, V> streamDescriptor;
  private final MapFunction<? super M, ? extends K> keyExtractor;
  private final MapFunction<? super M, ? extends V> msgExtractor;

  public OutputStreamImpl(StreamDescriptor.Output<K, V> streamDesc, MapFunction<? super M, ? extends K> keyExtractor, MapFunction<? super M, ? extends V> msgExtractor) {
    this.streamDescriptor = streamDesc;
    this.keyExtractor = keyExtractor;
    this.msgExtractor = msgExtractor;
  }

  public StreamSpec getStreamSpec() {
    return streamDescriptor.getStreamSpec();
  }

  public MapFunction<? super M, ? extends K> getKeyExtractor() {
    return this.keyExtractor;
  }

  public MapFunction<? super M, ? extends V> getMsgExtractor() {
    return this.msgExtractor;
  }

}
