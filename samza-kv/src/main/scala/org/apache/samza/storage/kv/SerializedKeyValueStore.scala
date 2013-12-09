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
  msgSerde: Serde[V],
  metrics: SerializedKeyValueStoreMetrics = new SerializedKeyValueStoreMetrics) extends KeyValueStore[K, V] with Logging {

  def get(key: K): V = {
    val keyBytes = bytesNotNull(key, keySerde)
    val found = store.get(keyBytes)
    metrics.gets.inc
    metrics.bytesSerialized.inc(keyBytes.size)
    if (found == null) {
      null.asInstanceOf[V]
    } else {
      metrics.bytesDeserialized.inc(found.size)
      msgSerde.fromBytes(found).asInstanceOf[V]
    }
  }

  def put(key: K, value: V) {
    metrics.puts.inc
    val keyBytes = bytesNotNull(key, keySerde)
    val valBytes = bytesNotNull(value, msgSerde)
    metrics.bytesSerialized.inc(keyBytes.size + valBytes.size)
    store.put(keyBytes, valBytes)
  }

  def putAll(entries: java.util.List[Entry[K, V]]) {
    val list = new java.util.ArrayList[Entry[Array[Byte], Array[Byte]]](entries.size())
    val iter = entries.iterator
    var bytesSerialized = 0L
    while (iter.hasNext) {
      val curr = iter.next
      val keyBytes = bytesNotNull(curr.getKey, keySerde)
      val valBytes = bytesNotNull(curr.getValue, msgSerde)
      bytesSerialized += keyBytes.size
      if (valBytes != null) {
        bytesSerialized += valBytes.size
      }
      list.add(new Entry(keyBytes, valBytes))
    }
    store.putAll(list)
    metrics.puts.inc(list.size)
    metrics.bytesSerialized.inc(bytesSerialized)
  }

  def delete(key: K) {
    metrics.deletes.inc
    val keyBytes = bytesNotNull(key, keySerde)
    metrics.bytesSerialized.inc(keyBytes.size)
    store.delete(keyBytes)
  }

  def range(from: K, to: K): KeyValueIterator[K, V] = {
    metrics.ranges.inc
    val fromBytes = bytesNotNull(from, keySerde)
    val toBytes = bytesNotNull(to, keySerde)
    metrics.bytesSerialized.inc(fromBytes.size + toBytes.size)
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
      val keyBytes = nxt.getKey
      val valBytes = nxt.getValue
      val key = if (keyBytes != null) {
        metrics.bytesDeserialized.inc(keyBytes.size)
        keySerde.fromBytes(keyBytes).asInstanceOf[K]
      } else {
        warn("Got a null key while iterating over a store. This is highly unexpected, since null in key and value is disallowed for key value stores.")
        null.asInstanceOf[K]
      }
      val value = if (valBytes != null) {
        metrics.bytesDeserialized.inc(valBytes.size)
        msgSerde.fromBytes(valBytes)
      } else {
        null.asInstanceOf[V]
      }
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

  /**
   * Null is not allowed for keys and values because some change log systems
   * (Kafka) model deletes as null.
   */
  private def bytesNotNull[T](t: T, serde: Serde[T]): Array[Byte] = if (t != null) {
    serde.toBytes(t)
  } else {
    throw new NullPointerException("Null is not a valid key or value.")
  }
}
