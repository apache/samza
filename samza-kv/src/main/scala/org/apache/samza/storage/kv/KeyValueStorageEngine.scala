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

import org.apache.samza.util.Logging
import org.apache.samza.storage.StorageEngine
import org.apache.samza.system.IncomingMessageEnvelope

import scala.collection.JavaConversions._

/**
 * A key value store.
 *
 * This implements both the key/value interface and the storage engine interface.
 */
class KeyValueStorageEngine[K, V](
  wrapperStore: KeyValueStore[K, V],
  rawStore: KeyValueStore[Array[Byte], Array[Byte]],
  metrics: KeyValueStorageEngineMetrics = new KeyValueStorageEngineMetrics,
  batchSize: Int = 500) extends StorageEngine with KeyValueStore[K, V] with Logging {

  var count = 0

  /* delegate to underlying store */
  def get(key: K): V = {
    metrics.gets.inc
    wrapperStore.get(key)
  }

  def getAll(keys: java.util.List[K]): java.util.Map[K, V] = {
    metrics.gets.inc(keys.size)
    wrapperStore.getAll(keys)
  }

  def put(key: K, value: V) = {
    metrics.puts.inc
    wrapperStore.put(key, value)
  }

  def putAll(entries: java.util.List[Entry[K, V]]) = {
    metrics.puts.inc(entries.size)
    wrapperStore.putAll(entries)
  }

  def delete(key: K) = {
    metrics.deletes.inc
    wrapperStore.delete(key)
  }

  def deleteAll(keys: java.util.List[K]) = {
    metrics.deletes.inc(keys.size)
    wrapperStore.deleteAll(keys)
  }

  def range(from: K, to: K) = {
    metrics.ranges.inc
    wrapperStore.range(from, to)
  }

  def all() = {
    metrics.alls.inc
    wrapperStore.all()
  }

  /**
   * Restore the contents of this key/value store from the change log,
   * batching updates to underlying raw store to skip wrapping functions for efficiency.
   */
  def restore(envelopes: java.util.Iterator[IncomingMessageEnvelope]) {
    val batch = new java.util.ArrayList[Entry[Array[Byte], Array[Byte]]](batchSize)

    for (envelope <- envelopes) {
      val keyBytes = envelope.getKey.asInstanceOf[Array[Byte]]
      val valBytes = envelope.getMessage.asInstanceOf[Array[Byte]]

      batch.add(new Entry(keyBytes, valBytes))

      if (batch.size >= batchSize) {
        rawStore.putAll(batch)
        batch.clear()
      }

      if (valBytes != null) {
        metrics.restoredBytes.inc(valBytes.size)
      }

      metrics.restoredBytes.inc(keyBytes.size)
      metrics.restoredMessages.inc
      count += 1

      if (count % 1000000 == 0) {
        info(count + " entries restored...")
      }
    }

    if (batch.size > 0) {
      rawStore.putAll(batch)
    }
  }

  def flush() = {
    trace("Flushing.")

    metrics.flushes.inc

    wrapperStore.flush()
  }

  def stop() = {
    trace("Stopping.")

    close()
  }

  def close() = {
    trace("Closing.")

    flush()
    wrapperStore.close()
  }
}
