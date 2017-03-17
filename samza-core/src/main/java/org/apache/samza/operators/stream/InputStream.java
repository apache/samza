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
package org.apache.samza.operators.stream;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;

import java.util.function.BiFunction;

/**
 * The interface for an input stream.
 *
 * @param <M> the type of messages in the input stream
 */
@InterfaceStability.Unstable
public interface InputStream<K, V, M> extends MessageStream<M> {

  StreamSpec getStreamSpec();

  Serde<K> getKeySerde();

  Serde<V> getMsgSerde();

  BiFunction<K, V, M> getMsgBuilder();

}
