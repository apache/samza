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
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.util.{LexicographicComparator, Logging}
import org.rocksdb.{TtlDB, _}

object RocksDbKeyValueStore extends Logging {

  def openDB(dir: File, options: Options, storeConfig: Config, isLoggedStore: Boolean,
             storeName: String, metrics: KeyValueStoreMetrics): RocksDB = {
    var ttl = 0L
    var useTTL = false

    if (storeConfig.containsKey("rocksdb.ttl.ms")) {
      try {
        ttl = storeConfig.getLong("rocksdb.ttl.ms")

        if (ttl > 0) {
          if (ttl < 1000) {
            warn("The ttl value requested for %s is %d which is less than 1000 (minimum). " +
              "Using 1000 ms instead.", storeName, ttl)
            ttl = 1000
          }
          ttl = TimeUnit.MILLISECONDS.toSeconds(ttl)
        } else {
          warn("Non-positive TTL for RocksDB implies infinite TTL for the data. " +
            "More Info - https://github.com/facebook/rocksdb/wiki/Time-to-Live")
        }

        useTTL = true
        if (isLoggedStore) {
          warn("%s is a TTL based store. Changelog is not supported for TTL based stores. " +
            "Use at your own discretion." format storeName)
        }
      } catch {
        case nfe: NumberFormatException =>
          throw new SamzaException("rocksdb.ttl.ms configuration value %s for store %s is not a number."
            format (storeConfig.get("rocksdb.ttl.ms"), storeName), nfe)
      }
    }

    try {
      val rocksDb =
        if (useTTL) {
          info("Opening RocksDB store with TTL value: %s" format ttl)
          TtlDB.open(options, dir.toString, ttl.toInt, false)
        } else {
          RocksDB.open(options, dir.toString)
        }

      if (storeConfig.containsKey("rocksdb.metrics.list")) {
        storeConfig
          .get("rocksdb.metrics.list")
          .split(",")
          .map(property => property.trim)
          .foreach(property => metrics.newGauge(property, () => rocksDb.getProperty(property)))
      }

      rocksDb
    } catch {
      case rocksDBException: RocksDBException =>
        throw new SamzaException("Error opening RocksDB store %s at location %s" format (storeName, dir.toString),
          rocksDBException)
    }
  }
}

class RocksDbKeyValueStore(
  val dir: File,
  val options: Options,
  val storeConfig: Config,
  val isLoggedStore: Boolean,
  val storeName: String,
  val writeOptions: WriteOptions = new WriteOptions(),
  val flushOptions: FlushOptions = new FlushOptions(),
  val metrics: KeyValueStoreMetrics = new KeyValueStoreMetrics) extends KeyValueStore[Array[Byte], Array[Byte]] with Logging {

  // lazy val here is important because the store directories do not exist yet, it can only be opened
  // after the directories are created, which happens much later from now.
  private lazy val db = RocksDbKeyValueStore.openDB(dir, options, storeConfig, isLoggedStore, storeName, metrics)
  private val lexicographic = new LexicographicComparator()

  /**
    * null while the store is open. Set to an Exception holding the stacktrace at the time of first close by #close.
    * Reads and writes to this field must be guarded by stateChangeLock.
    * This is an Exception instead of an Array[StackTraceElement] for ease of logging.
    */
  private var stackAtFirstClose: Exception = null
  private val stateChangeLock = new ReentrantReadWriteLock()

  def get(key: Array[Byte]): Array[Byte] = ifOpen {
    metrics.gets.inc
    require(key != null, "Null key not allowed.")
    val found = db.get(key)
    if (found != null) {
      metrics.bytesRead.inc(found.length)
    }
    found
  }

  override def getAll(keys: java.util.List[Array[Byte]]): java.util.Map[Array[Byte], Array[Byte]] = ifOpen {
    metrics.getAlls.inc
    require(keys != null, "Null keys not allowed.")
    val map = db.multiGet(keys)
    if (map != null) {
      var bytesRead = 0L
      val iterator = map.values().iterator
      while (iterator.hasNext) {
        val value = iterator.next
        if (value != null) {
          bytesRead += value.length
        }
      }
      metrics.bytesRead.inc(bytesRead)
    }
    map
  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = ifOpen {
    metrics.puts.inc
    require(key != null, "Null key not allowed.")
    if (value == null) {
      db.delete(writeOptions, key)
    } else {
      metrics.bytesWritten.inc(key.length + value.length)
      db.put(writeOptions, key, value)
    }
  }

  // Write batch from RocksDB API is not used currently because of: https://github.com/facebook/rocksdb/issues/262
  def putAll(entries: java.util.List[Entry[Array[Byte], Array[Byte]]]): Unit = ifOpen {
    val iter = entries.iterator
    var wrote = 0
    var deletes = 0
    while (iter.hasNext) {
      wrote += 1
      val curr = iter.next()
      if (curr.getValue == null) {
        deletes += 1
        db.delete(writeOptions, curr.getKey)
      } else {
        val key = curr.getKey
        val value = curr.getValue
        metrics.bytesWritten.inc(key.length + value.length)
        db.put(writeOptions, key, value)
      }
    }
    metrics.puts.inc(wrote)
    metrics.deletes.inc(deletes)
  }

  def delete(key: Array[Byte]): Unit = ifOpen {
    metrics.deletes.inc
    put(key, null)
  }

  def range(from: Array[Byte], to: Array[Byte]): KeyValueIterator[Array[Byte], Array[Byte]] = ifOpen {
    metrics.ranges.inc
    require(from != null && to != null, "Null bound not allowed.")
    new RocksDbRangeIterator(db.newIterator(), from, to)
  }

  def all(): KeyValueIterator[Array[Byte], Array[Byte]] = ifOpen {
    metrics.alls.inc
    val iter = db.newIterator()
    iter.seekToFirst()
    new RocksDbIterator(iter)
  }

  def flush(): Unit = ifOpen {
    metrics.flushes.inc
    trace("Flushing store: %s" format storeName)
    db.flush(flushOptions)
    trace("Flushed store: %s" format storeName)
  }

  def close(): Unit = {
    stateChangeLock.writeLock().lock()
    try {
      trace("Closing.")
      if (stackAtFirstClose == null) { // first close
        stackAtFirstClose = new Exception()
        db.close()
      } else {
        warn(new SamzaException("Close called again on a closed store: %s. Ignoring this close." +
          "Stack at first close is under 'Caused By'." format storeName, stackAtFirstClose))
      }
    } finally {
      stateChangeLock.writeLock().unlock()
    }
  }

  private def ifOpen[T](fn: => T): T = {
    stateChangeLock.readLock().lock()
    try {
      if (stackAtFirstClose == null) {
        fn
      } else {
        throw new SamzaException("Attempted to access a closed store: %s. " +
          "Stack at first close is under 'Caused By'." format storeName, stackAtFirstClose)
      }
    } finally {
      stateChangeLock.readLock().unlock()
    }
  }

  class RocksDbIterator(iter: RocksIterator) extends KeyValueIterator[Array[Byte], Array[Byte]] {
    private var open = true

    override def close() = ifOpen {
      open = false
      iter.close()
    }

    override def remove() = throw new UnsupportedOperationException("RocksDB iterator doesn't support remove")

    override def hasNext() = ifOpen(iter.isValid)

    // The iterator is already pointing to the next element
    protected def peekKey() = ifOpen {
      getEntry().getKey
    }

    protected def getEntry() = ifOpen {
      val key = iter.key
      val value = iter.value
      new Entry(key, value)
    }

    // By virtue of how RocksdbIterator is implemented, the implementation of
    // our iterator is slightly different from standard java iterator next will
    // always point to the current element, when next is called, we return the
    // current element we are pointing to and advance the iterator to the next
    // location (The new location may or may not be valid - this will surface
    // when the next next() call is made, the isValid will fail)
    override def next(): Entry[Array[Byte], Array[Byte]] = ifOpen {
      if (!hasNext()) {
        throw new NoSuchElementException
      }

      val entry = getEntry()
      iter.next()
      metrics.bytesRead.inc(entry.getKey.length)
      if (entry.getValue != null) {
        metrics.bytesRead.inc(entry.getValue.length)
      }
      entry
    }

    override def finalize(): Unit = ifOpen {
      if (open) {
        trace("Leaked reference to RocksDB iterator, forcing close.")
        close()
      }
    }
  }

  class RocksDbRangeIterator(iter: RocksIterator, from: Array[Byte], to: Array[Byte]) extends RocksDbIterator(iter) {
    // RocksDB's JNI interface does not expose getters/setters that allow the
    // comparator to be pluggable, and the default is lexicographic, so it's
    // safe to just force lexicographic comparator here for now.
    val comparator: LexicographicComparator = lexicographic
    ifOpen(iter.seek(from))

    override def hasNext() = ifOpen {
      super.hasNext() && comparator.compare(peekKey(), to) < 0
    }
  }
}
