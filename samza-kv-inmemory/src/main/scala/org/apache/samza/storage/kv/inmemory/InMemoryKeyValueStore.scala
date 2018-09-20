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
package org.apache.samza.storage.kv.inmemory

import com.google.common.primitives.UnsignedBytes
import org.apache.samza.util.Logging
import org.apache.samza.storage.kv._
import java.util

/**
 * In memory implementation of a key value store.
 *
 * This uses a ConcurrentSkipListMap to store the keys in order
 *
 * @param metrics A metrics instance to publish key-value store related statistics
 */
class InMemoryKeyValueStore(val metrics: KeyValueStoreMetrics = new KeyValueStoreMetrics)
  extends KeyValueStore[Array[Byte], Array[Byte]] with Logging {

  val underlying = new util.concurrent.ConcurrentSkipListMap[Array[Byte], Array[Byte]] (UnsignedBytes.lexicographicalComparator())

  override def flush(): Unit = {
    // No-op for In memory store.
    metrics.flushes.inc
  }

  override def close(): Unit = Unit

  private class InMemoryIterator (val iter: util.Iterator[util.Map.Entry[Array[Byte], Array[Byte]]])
    extends KeyValueIterator[Array[Byte], Array[Byte]] {

    override def close(): Unit = Unit

    override def remove(): Unit = throw new UnsupportedOperationException("InMemoryKeyValueStore iterator doesn't support remove")

    override def next(): Entry[Array[Byte], Array[Byte]] = {
      val n = iter.next()
      if (n != null && n.getKey != null) {
        metrics.bytesRead.inc(n.getKey.size)
      }
      if (n != null && n.getValue != null) {
        metrics.bytesRead.inc(n.getValue.size)
      }
      new Entry(n.getKey, n.getValue)
    }

    override def hasNext: Boolean = iter.hasNext
  }

  override def all(): KeyValueIterator[Array[Byte], Array[Byte]] = {
    metrics.alls.inc

    new InMemoryIterator(underlying.entrySet().iterator())
  }

  override def range(from: Array[Byte], to: Array[Byte]): KeyValueIterator[Array[Byte], Array[Byte]] = {
    metrics.ranges.inc
    require(from != null && to != null, "Null bound not allowed.")

    new InMemoryIterator(underlying.subMap(from, to).entrySet().iterator())
  }

  override def delete(key: Array[Byte]): Unit = {
    metrics.deletes.inc
    put(key, null)
  }

  override def putAll(entries: util.List[Entry[Array[Byte], Array[Byte]]]): Unit = {
    // TreeMap's putAll requires a map, so we'd need to iterate over all the entries anyway
    // to use it, in order to putAll here.  Therefore, just iterate here.
    val iter = entries.iterator()
    while(iter.hasNext) {
      val next = iter.next()
      put(next.getKey, next.getValue)
    }
  }

  override def put(key: Array[Byte], value: Array[Byte]): Unit = {
    metrics.puts.inc
    require(key != null, "Null key not allowed.")
    if (value == null) {
      metrics.deletes.inc
      underlying.remove(key)
    } else {
      metrics.bytesWritten.inc(key.size + value.size)
      underlying.put(key, value)
    }
  }

  override def get(key: Array[Byte]): Array[Byte] = {
    metrics.gets.inc
    require(key != null, "Null key not allowed.")
    val found = underlying.get(key)
    if (found != null) {
      metrics.bytesRead.inc(found.size)
    }
    found
  }

  override def snapshot(from: Array[Byte], to: Array[Byte]): KeyValueSnapshot[Array[Byte], Array[Byte]] = {
    // snapshot the underlying map
    val entries = underlying.subMap(from, to).entrySet()
    new KeyValueSnapshot[Array[Byte], Array[Byte]] {
      override def iterator(): KeyValueIterator[Array[Byte], Array[Byte]] = {
        new InMemoryIterator(entries.iterator())
      }

      override def close() { }
    }
  }
}
