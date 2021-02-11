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
import org.apache.samza.system.{ChangelogSSPIterator, OutgoingMessageEnvelope, SystemStreamPartition}
import org.apache.samza.task.{MessageCollector, TaskInstanceCollector}
import org.apache.samza.util.TimerUtil
import java.nio.file.Path
import java.util.Optional

import com.google.common.annotations.VisibleForTesting
import org.apache.samza.checkpoint.CheckpointId

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
  changelogSSP: SystemStreamPartition,
  changelogCollector: MessageCollector,
  metrics: KeyValueStorageEngineMetrics = new KeyValueStorageEngineMetrics,
  batchSize: Int = 500,
  val clock: () => Long = { System.nanoTime }) extends StorageEngine with KeyValueStore[K, V] with TimerUtil with Logging {

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
   * Restore the contents of this key/value store from the change log, batching updates to underlying raw store
   * for efficiency.
   *
   * With transactional state disabled, iterator mode will always be 'restore'. With transactional state enabled,
   * iterator mode may switch from 'restore' to 'trim' at some point, but will not switch back to 'restore'.
   */
  def restore(iterator: ChangelogSSPIterator) {
    info("Restoring entries for store: " + storeName + " in directory: " + storeDir.toString)
    var restoredMessages = 0
    var restoredBytes = 0
    var trimmedMessages = 0
    var trimmedBytes = 0
    var previousMode = ChangelogSSPIterator.Mode.RESTORE

    val batch = new java.util.ArrayList[Entry[Array[Byte], Array[Byte]]](batchSize)
    var lastBatchFlushed = false

    while(iterator.hasNext && !Thread.currentThread().isInterrupted) {
      val envelope = iterator.next()
      val keyBytes = envelope.getKey.asInstanceOf[Array[Byte]]
      val valBytes = envelope.getMessage.asInstanceOf[Array[Byte]]
      val mode = iterator.getMode

      if (mode.equals(ChangelogSSPIterator.Mode.RESTORE)) {
        if (previousMode == ChangelogSSPIterator.Mode.TRIM) {
          throw new IllegalStateException(
            String.format("Illegal ChangelogSSPIterator mode change from TRIM to RESTORE for store: %s " +
              "in dir: %s with changelog SSP: {}.", storeName, storeDir, changelogSSP))
        }
        batch.add(new Entry(keyBytes, valBytes))

        if (batch.size >= batchSize) {
          doPutAll(rawStore, batch)
          batch.clear()
        }

        // update metrics
        restoredMessages += 1
        restoredBytes += keyBytes.length
        if (valBytes != null) restoredBytes += valBytes.length
        metrics.restoredMessagesGauge.set(restoredMessages)
        metrics.restoredBytesGauge.set(restoredBytes)

        // log progress every million messages
        if (restoredMessages % 1000000 == 0) {
          info(restoredMessages + " entries restored for store: " + storeName + " in directory: " + storeDir.toString + "...")
        }
      } else {
        // first write any open restore batches to store
        if (!lastBatchFlushed) {
          info(restoredMessages + " total entries restored for store: " + storeName + " in directory: " + storeDir.toString + ".")
          if (batch.size > 0) {
            doPutAll(rawStore, batch)
            batch.clear()
          }
          lastBatchFlushed = true
        }

        // then overwrite the value to be trimmed with its current store value
        val currentValBytes = rawStore.get(keyBytes)
        val changelogMessage = new OutgoingMessageEnvelope(
          changelogSSP.getSystemStream, changelogSSP.getPartition, keyBytes, currentValBytes)
        changelogCollector.send(changelogMessage)

        // update metrics
        trimmedMessages += 1
        trimmedBytes += keyBytes.length
        if (currentValBytes != null) trimmedBytes += currentValBytes.length
        metrics.trimmedMessagesGauge.set(trimmedMessages)
        metrics.trimmedBytesGauge.set(trimmedBytes)

        // log progress every hundred thousand messages
        if (trimmedMessages % 100000 == 0) {
          info(trimmedMessages + " entries trimmed for store: " + storeName + " in directory: " + storeDir.toString + "...")
        }
      }

      previousMode = mode
    }

    // if the last batch isn't flushed yet (e.g., for non transactional state or no messages to trim), flush it now
    if (!lastBatchFlushed) {
      info(restoredMessages + " total entries restored for store: " + storeName + " in directory: " + storeDir.toString + ".")
      if (batch.size > 0) {
        doPutAll(rawStore, batch)
        batch.clear()
      }
      lastBatchFlushed = true
    }
    info(trimmedMessages + " entries trimmed for store: " + storeName + " in directory: " + storeDir.toString + ".")

    // flush the store and the changelog producer
    flush() // TODO HIGH pmaheshw SAMZA-2338: Need a way to flush changelog producers. This only flushes the stores.

    if (Thread.currentThread().isInterrupted) {
      warn("Received an interrupt during store restoration. Exiting without restoring the full state.")
      throw new InterruptedException("Received an interrupt during store restoration.")
    }
  }

  def flush() = {
    updateTimer(metrics.flushNs) {
      trace("Flushing.")
      metrics.flushes.inc
      wrapperStore.flush()
    }
  }

  def checkpoint(id: CheckpointId): Optional[Path] = {
    updateTimer(metrics.checkpointNs) {
      trace("Checkpointing.")
      metrics.checkpoints.inc
      wrapperStore.checkpoint(id)
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

  @VisibleForTesting
  private[kv] def getRawStore: KeyValueStore[Array[Byte], Array[Byte]] = {
    rawStore
  }

  @VisibleForTesting
  private[kv] def getWrapperStore: KeyValueStore[K, V] = {
    wrapperStore
  }
}
