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
 *//*
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
package org.apache.samza.serializers

import org.apache.samza.SamzaException
import org.apache.samza.operators.KV

object KVSerde {
  def of[K, V](keySerde: Serde[K], valueSerde: Serde[V]) = new KVSerde[K, V](keySerde, valueSerde)
}

/**
  * A marker serde class to indicate that messages are keyed and should be deserialized as K-V pairs.
  * This class is intended for use cases where a single Serde parameter or configuration is required.
  *
  * @tparam K type of the key in the message
  * @tparam V type of the value in the message
  */
class KVSerde[K, V](keySerde: Serde[K], valueSerde: Serde[V]) extends Serde[KV[K, V]] {
  /**
    * Implementation Note: This serde must not be used by the framework for serialization/deserialization directly.
    * Wire up and use the constituent keySerde and valueSerde instead.
    */
  override def fromBytes(bytes: Array[Byte]): Nothing = {
    throw new NotImplementedError("This is a marker serde and must not be used directly. " +
      "Samza must wire up and use the keySerde and valueSerde instead.")
  }

  override def toBytes(`object`: KV[K, V]): Nothing = {
    throw new SamzaException("This is a marker serde and must not be used directly. " +
      "Samza must wire up and use the keySerde and valueSerde instead.")
  }

  def getKeySerde: Serde[K] = keySerde

  def getValueSerde: Serde[V] = valueSerde
}