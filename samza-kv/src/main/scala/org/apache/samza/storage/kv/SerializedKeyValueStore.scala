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

import grizzled.slf4j.Logging
import org.apache.samza.serializers._

/**
 * A key-value store wrapper that handles serialization
 */
class SerializedKeyValueStore[K, V](
  store: KeyValueStore[Array[Byte], Array[Byte]],
  keySerde: Serde[K],
  msgSerde: Serde[V],
  metrics: SerializedKeyValueStoreMetrics = new SerializedKeyValueStoreMetrics) extends KeyValueStore[K, V] with Logging {

  def get(key: K): V = {
    val keyBytes = toBytesOrNull(key, keySerde)
    val found = store.get(keyBytes)
    metrics.gets.inc
    fromBytesOrNull(found, msgSerde)
  }

  def put(key: K, value: V) {
    metrics.puts.inc
    val keyBytes = toBytesOrNull(key, keySerde)
    val valBytes = toBytesOrNull(value, msgSerde)
    store.put(keyBytes, valBytes)
  }

  def putAll(entries: java.util.List[Entry[K, V]]) {
    val list = new java.util.ArrayList[Entry[Array[Byte], Array[Byte]]](entries.size())
    val iter = entries.iterator
    while (iter.hasNext) {
      val curr = iter.next
      val keyBytes = toBytesOrNull(curr.getKey, keySerde)
      val valBytes = toBytesOrNull(curr.getValue, msgSerde)
      list.add(new Entry(keyBytes, valBytes))
    }
    store.putAll(list)
    metrics.puts.inc(list.size)
  }

  def delete(key: K) {
    metrics.deletes.inc
    val keyBytes = toBytesOrNull(key, keySerde)
    store.delete(keyBytes)
  }

  def range(from: K, to: K): KeyValueIterator[K, V] = {
    metrics.ranges.inc
    val fromBytes = toBytesOrNull(from, keySerde)
    val toBytes = toBytesOrNull(to, keySerde)
    new DeserializingIterator(store.range(fromBytes, toBytes))
  }

  def all(): KeyValueIterator[K, V] = {
    metrics.alls.inc
    new DeserializingIterator(store.all)
  }

  private class DeserializingIterator(iter: KeyValueIterator[Array[Byte], Array[Byte]]) extends KeyValueIterator[K, V] {
    def hasNext() = iter.hasNext()
    def remove() = iter.remove()
    def close() = iter.close()
    def next(): Entry[K, V] = {
      val nxt = iter.next()
      val key = fromBytesOrNull(nxt.getKey, keySerde)
      val value = fromBytesOrNull(nxt.getValue, msgSerde)
      new Entry(key, value)
    }
  }

  def flush {
    trace("Flushing.")

    metrics.flushes.inc

    store.flush
  }

  def close {
    trace("Closing.")

    store.close
  }

  def toBytesOrNull[T](t: T, serde: Serde[T]): Array[Byte] = if (t == null) {
    null
  } else {
    val bytes = serde.toBytes(t)
    metrics.bytesSerialized.inc(bytes.size)
    bytes
  }

  def fromBytesOrNull[T](bytes: Array[Byte], serde: Serde[T]): T = if (bytes == null) {
    null.asInstanceOf[T]
  } else {
    val obj = serde.fromBytes(bytes)
    metrics.bytesDeserialized.inc(bytes.size)
    obj
  }
}
