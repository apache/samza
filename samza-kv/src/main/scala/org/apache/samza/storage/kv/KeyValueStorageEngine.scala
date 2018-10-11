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

import java.io.File

import org.apache.samza.util.Logging
import org.apache.samza.storage.{StorageEngine, StoreProperties}
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.util.TimerUtil

import scala.collection.JavaConverters._

/**
 * A key value store.
 *
 * This implements both the key/value interface and the storage engine interface.
 */
class KeyValueStorageEngine[K, V](
  storeName: String,
  storeDir: File,
  storeProperties: StoreProperties,
  wrapperStore: KeyValueStore[K, V],
  rawStore: KeyValueStore[Array[Byte], Array[Byte]],
  metrics: KeyValueStorageEngineMetrics = new KeyValueStorageEngineMetrics,
  batchSize: Int = 500,
  val clock: () => Long = { System.nanoTime }) extends StorageEngine with KeyValueStore[K, V] with TimerUtil with Logging {

  var count = 0

  /* delegate to underlying store */
  def get(key: K): V = {
    updateTimer(metrics.getNs) {
      metrics.gets.inc

      //update the duration and return the fetched value
      wrapperStore.get(key)
    }
  }

  override def getAll(keys: java.util.List[K]): java.util.Map[K, V] = {
    updateTimer(metrics.getAllNs) {
      metrics.getAlls.inc()
      metrics.gets.inc(keys.size)
      wrapperStore.getAll(keys)
    }
  }

  def put(key: K, value: V) = {
    updateTimer(metrics.putNs) {
      metrics.puts.inc
      wrapperStore.put(key, value)
    }
  }

  def putAll(entries: java.util.List[Entry[K, V]]) = {
    doPutAll(wrapperStore, entries)
  }

  def delete(key: K) = {
    updateTimer(metrics.deleteNs) {
      metrics.deletes.inc
      wrapperStore.delete(key)
    }
  }

  override def deleteAll(keys: java.util.List[K]) = {
    updateTimer(metrics.deleteAllNs) {
      metrics.deleteAlls.inc()
      metrics.deletes.inc(keys.size)
      wrapperStore.deleteAll(keys)
    }
  }

  def range(from: K, to: K) = {
    updateTimer(metrics.rangeNs) {
      metrics.ranges.inc
      wrapperStore.range(from, to)
    }
  }

  def all() = {
    updateTimer(metrics.allNs) {
      metrics.alls.inc
      wrapperStore.all()
    }
  }

  /**
   * Restore the contents of this key/value store from the change log,
   * batching updates to underlying raw store to notAValidEvent wrapping functions for efficiency.
   */
  def restore(envelopes: java.util.Iterator[IncomingMessageEnvelope]) {
    info("Restoring entries for store: " + storeName + " in directory: " + storeDir.toString)

    val batch = new java.util.ArrayList[Entry[Array[Byte], Array[Byte]]](batchSize)

    for (envelope <- envelopes.asScala) {
      val keyBytes = envelope.getKey.asInstanceOf[Array[Byte]]
      val valBytes = envelope.getMessage.asInstanceOf[Array[Byte]]

      batch.add(new Entry(keyBytes, valBytes))

      if (batch.size >= batchSize) {
        doPutAll(rawStore, batch)
        batch.clear()
      }

      if (valBytes != null) {
        metrics.restoredBytes.inc(valBytes.length)
        metrics.restoredBytesGauge.set(metrics.restoredBytesGauge.getValue + valBytes.length)
      }

      metrics.restoredBytes.inc(keyBytes.length)
      metrics.restoredBytesGauge.set(metrics.restoredBytesGauge.getValue + keyBytes.length)

      metrics.restoredMessages.inc()
      metrics.restoredMessagesGauge.set(metrics.restoredMessagesGauge.getValue + 1)
      count += 1

      if (count % 1000000 == 0) {
        info(count + " entries restored for store: " + storeName + " in directory: " + storeDir.toString + "...")
      }
    }

    info(count + " total entries restored for store: " + storeName + " in directory: " + storeDir.toString + ".")

    if (batch.size > 0) {
      doPutAll(rawStore, batch)
    }
  }

  def flush() = {
    updateTimer(metrics.flushNs) {
      trace("Flushing.")
      metrics.flushes.inc
      wrapperStore.flush()
    }
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

  private def doPutAll[Key, Value](store: KeyValueStore[Key, Value], entries: java.util.List[Entry[Key, Value]]) = {
    updateTimer(metrics.putAllNs) {
      metrics.putAlls.inc()
      metrics.puts.inc(entries.size)
      store.putAll(entries)
    }
  }

  override def getStoreProperties: StoreProperties = storeProperties

  override def snapshot(from: K, to: K): KeyValueSnapshot[K, V] = {
    updateTimer(metrics.snapshotNs) {
      metrics.snapshots.inc
      wrapperStore.snapshot(from, to)
    }
  }
}
