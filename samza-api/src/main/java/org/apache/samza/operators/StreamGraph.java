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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;

import java.util.Map;

/**
 * Job-level programming interface to create an operator DAG and run in various different runtime environments.
 */
@InterfaceStability.Unstable
public interface StreamGraph {
  /**
   * Method to add an input {@link MessageStream} from the system
   *
   * @param streamSpec  the {@link StreamSpec} describing the physical characteristics of the input {@link MessageStream}
   * @param keySerde  the serde used to serialize/deserialize the message key from the input {@link MessageStream}
   * @param msgSerde  the serde used to serialize/deserialize the message body from the input {@link MessageStream}
   * @param <K>  the type of key in the input message
   * @param <V>  the type of message in the input message
   * @param <M>  the type of {@link MessageEnvelope} in the input {@link MessageStream}
   * @return   the input {@link MessageStream} object
   */
  <K, V, M extends MessageEnvelope<K, V>> MessageStream<M> createInStream(StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde);

  /**
   * Method to add an output {@link MessageStream} from the system
   *
   * @param streamSpec  the {@link StreamSpec} describing the physical characteristics of the output {@link MessageStream}
   * @param keySerde  the serde used to serialize/deserialize the message key from the output {@link MessageStream}
   * @param msgSerde  the serde used to serialize/deserialize the message body from the output {@link MessageStream}
   * @param <K>  the type of key in the output message
   * @param <V>  the type of message in the output message
   * @param <M>  the type of {@link MessageEnvelope} in the output {@link MessageStream}
   * @return   the output {@link MessageStream} object
   */
  <K, V, M extends MessageEnvelope<K, V>> OutputStream<M> createOutStream(StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde);

  /**
   * Method to add an intermediate {@link MessageStream} from the system
   *
   * @param streamSpec  the {@link StreamSpec} describing the physical characteristics of the intermediate {@link MessageStream}
   * @param keySerde  the serde used to serialize/deserialize the message key from the intermediate {@link MessageStream}
   * @param msgSerde  the serde used to serialize/deserialize the message body from the intermediate {@link MessageStream}
   * @param <K>  the type of key in the intermediate message
   * @param <V>  the type of message in the intermediate message
   * @param <M>  the type of {@link MessageEnvelope} in the intermediate {@link MessageStream}
   * @return   the intermediate {@link MessageStream} object
   */
  <K, V, M extends MessageEnvelope<K, V>> OutputStream<M> createIntStream(StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde);

  /**
   * Method to get the input {@link MessageStream}s
   *
   * @return the input {@link MessageStream}
   */
  Map<StreamSpec, MessageStream> getInStreams();

  /**
   * Method to get the {@link OutputStream}s
   *
   * @return  the map of all {@link OutputStream}s
   */
  Map<StreamSpec, OutputStream> getOutStreams();

  /**
   * Method to set the {@link ContextManager} for this {@link StreamGraph}
   *
   * @param manager  the {@link ContextManager} object
   * @return  this {@link StreamGraph} object
   */
  StreamGraph withContextManager(ContextManager manager);

}
