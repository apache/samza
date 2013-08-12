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

package org.apache.samza.storage.kv

import java.util.Iterator
import org.apache.samza.serializers._
import grizzled.slf4j.Logging

/**
 * A key-value store wrapper that handles serialization
 */
class SerializedKeyValueStore[K, V](
  store: KeyValueStore[Array[Byte], Array[Byte]],
  keySerde: Serde[K],
  msgSerde: Serde[V]) extends KeyValueStore[K, V] with Logging {

  def get(key: K): V = {
    val keyBytes = keySerde.toBytes(key)
    val found = store.get(keyBytes)
    if (found == null)
      null.asInstanceOf[V]
    else
      msgSerde.fromBytes(found).asInstanceOf[V]
  }

  def put(key: K, value: V) {
    val keyBytes = keySerde.toBytes(key)
    val valBytes = msgSerde.toBytes(value)
    store.put(keyBytes, valBytes)
  }

  def putAll(entries: java.util.List[Entry[K, V]]) {
    val list = new java.util.ArrayList[Entry[Array[Byte], Array[Byte]]](entries.size())
    val iter = entries.iterator
    while (iter.hasNext) {
      val curr = iter.next
      val keyBytes = keySerde.toBytes(curr.getKey)
      val valBytes = msgSerde.toBytes(curr.getValue)
      list.add(new Entry(keyBytes, valBytes))
    }
    store.putAll(list)
  }

  def delete(key: K) {
    store.delete(keySerde.toBytes(key))
  }

  def range(from: K, to: K): KeyValueIterator[K, V] = {
    val fromBytes = keySerde.toBytes(from)
    val toBytes = keySerde.toBytes(to)
    new DeserializingIterator(store.range(fromBytes, toBytes))
  }

  def all(): KeyValueIterator[K, V] =
    new DeserializingIterator(store.all)

  private class DeserializingIterator(iter: KeyValueIterator[Array[Byte], Array[Byte]]) extends KeyValueIterator[K, V] {
    def hasNext() = iter.hasNext()
    def remove() = iter.remove()
    def close() = iter.close()
    def next(): Entry[K, V] = {
      val nxt = iter.next()
      new Entry(keySerde.fromBytes(nxt.getKey()).asInstanceOf[K], msgSerde.fromBytes(nxt.getValue()).asInstanceOf[V])
    }
  }

  def flush {
    trace("Flushing.")

    store.flush
  }

  def close {
    trace("Closing.")

    store.close
  }
}
