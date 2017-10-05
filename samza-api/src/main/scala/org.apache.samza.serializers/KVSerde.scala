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

package org.apache.samza.serializers

import java.nio.ByteBuffer

import org.apache.samza.operators.KV

object KVSerde {
  def of[K, V](keySerde: Serde[K], valueSerde: Serde[V]) = new KVSerde[K, V](keySerde, valueSerde)
}

/**
  * A serde for [[KV]] key-value pairs.
  *
  * When this serde is used for streams in the High Level API, Samza wires up and uses the provided
  * keySerde and valueSerde for the keys and values in the stream separately. I.e., the fromBytes and toBytes
  * methods in this class aren't used directly for streams.
  *
  * @tparam K type of the key in the message
  * @tparam V type of the value in the message
  */
class KVSerde[K, V](keySerde: Serde[K], valueSerde: Serde[V]) extends Serde[KV[K, V]] {
  override def fromBytes(bytes: Array[Byte]): KV[K, V] = {
    val byteBuffer = ByteBuffer.wrap(bytes)
    val keyLength = byteBuffer.getInt()
    val keyBytes = new Array[Byte](keyLength)
    byteBuffer.get(keyBytes)
    val valueLength = byteBuffer.getInt()
    val valueBytes = new Array[Byte](valueLength)
    byteBuffer.get(valueBytes)
    val key = keySerde.fromBytes(keyBytes)
    val value = valueSerde.fromBytes(valueBytes)
    KV.of(key, value)
  }

  override def toBytes(obj: KV[K, V]): Array[Byte] = {
    val keyBytes = keySerde.toBytes(obj.key)
    val valueBytes = valueSerde.toBytes(obj.value)
    val bytes = new Array[Byte](8 + keyBytes.length + 8 + valueBytes.length)
    val byteBuffer = ByteBuffer.wrap(bytes)
    byteBuffer.putInt(keyBytes.length)
    byteBuffer.put(keyBytes)
    byteBuffer.putInt(valueBytes.length)
    byteBuffer.put(valueBytes)
    byteBuffer.array()
  }

  def getKeySerde: Serde[K] = keySerde

  def getValueSerde: Serde[V] = valueSerde
}