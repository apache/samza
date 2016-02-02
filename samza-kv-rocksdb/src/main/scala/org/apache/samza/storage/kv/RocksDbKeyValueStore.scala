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
import org.apache.samza.SamzaException
import org.apache.samza.util.{ LexicographicComparator, Logging }
import org.apache.samza.config.Config
import org.apache.samza.container.SamzaContainerContext
import org.rocksdb._
import org.rocksdb.TtlDB;

object RocksDbKeyValueStore extends Logging {

  def openDB(dir: File, options: Options, storeConfig: Config, isLoggedStore: Boolean, storeName: String): RocksDB = {
    var ttl = 0L
    var useTTL = false

    if (storeConfig.containsKey("rocksdb.ttl.ms"))
    {
      try
      {
        ttl = storeConfig.getLong("rocksdb.ttl.ms")

        // RocksDB accepts TTL in seconds, convert ms to seconds
        if (ttl < 1000)
        {
          warn("The ttl values requested for %s is %d, which is less than 1000 (minimum), using 1000 instead",
               storeName,
               ttl)
          ttl = 1000
        }
        ttl = ttl / 1000

        useTTL = true
        if (isLoggedStore)
        {
          error("%s is a TTL based store, changelog is not supported for TTL based stores, use at your own discretion" format storeName)
        }
      }
      catch
        {
          case nfe: NumberFormatException => throw new SamzaException("rocksdb.ttl.ms configuration is not a number, " + "value found %s" format storeConfig.get(
            "rocksdb.ttl.ms"))
        }
    }

    try
    {
      if (useTTL)
      {
        info("Opening RocksDB store with TTL value: %s" format ttl)
        TtlDB.open(options, dir.toString, ttl.toInt, false)
      }
      else
      {
        RocksDB.open(options, dir.toString)
      }
    }
    catch
      {
        case rocksDBException: RocksDBException =>
        {
          throw new SamzaException(
            "Error opening RocksDB store %s at location %s, received the following exception from RocksDB %s".format(
              storeName,
              dir.toString,
              rocksDBException))
        }
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
  private lazy val db = RocksDbKeyValueStore.openDB(dir, options, storeConfig, isLoggedStore, storeName)
  private val lexicographic = new LexicographicComparator()
  private var deletesSinceLastCompaction = 0

  def get(key: Array[Byte]): Array[Byte] = {
    metrics.gets.inc
    require(key != null, "Null key not allowed.")
    val found = db.get(key)
    if (found != null) {
      metrics.bytesRead.inc(found.size)
    }
    found
  }

  def getAll(keys: java.util.List[Array[Byte]]): java.util.Map[Array[Byte], Array[Byte]] = {
    metrics.getAlls.inc
    require(keys != null, "Null keys not allowed.")
    val map = db.multiGet(keys)
    if (map != null) {
      var bytesRead = 0L
      val iterator = map.values().iterator
      while (iterator.hasNext) {
        val value = iterator.next
        if (value != null) {
          bytesRead += value.size
        }
      }
      metrics.bytesRead.inc(bytesRead)
    }
    map
  }

  def put(key: Array[Byte], value: Array[Byte]) {
    metrics.puts.inc
    require(key != null, "Null key not allowed.")
    if (value == null) {
      db.remove(writeOptions, key)
      deletesSinceLastCompaction += 1
    } else {
      metrics.bytesWritten.inc(key.size + value.size)
      db.put(writeOptions, key, value)
    }
  }

  // Write batch from RocksDB API is not used currently because of: https://github.com/facebook/rocksdb/issues/262
  def putAll(entries: java.util.List[Entry[Array[Byte], Array[Byte]]]) {
    val iter = entries.iterator
    var wrote = 0
    var deletes = 0
    while (iter.hasNext) {
      wrote += 1
      val curr = iter.next()
      if (curr.getValue == null) {
        deletes += 1
        db.remove(writeOptions, curr.getKey)
      } else {
        val key = curr.getKey
        val value = curr.getValue
        metrics.bytesWritten.inc(key.size + value.size)
        db.put(writeOptions, key, value)
      }
    }
    metrics.puts.inc(wrote)
    metrics.deletes.inc(deletes)
    deletesSinceLastCompaction += deletes
  }

  def delete(key: Array[Byte]) {
    metrics.deletes.inc
    put(key, null)
  }

  def deleteAll(keys: java.util.List[Array[Byte]]) = {
    KeyValueStore.Extension.deleteAll(this, keys)
  }

  def range(from: Array[Byte], to: Array[Byte]): KeyValueIterator[Array[Byte], Array[Byte]] = {
    metrics.ranges.inc
    require(from != null && to != null, "Null bound not allowed.")
    new RocksDbRangeIterator(db.newIterator(), from, to)
  }

  def all(): KeyValueIterator[Array[Byte], Array[Byte]] = {
    metrics.alls.inc
    val iter = db.newIterator()
    iter.seekToFirst()
    new RocksDbIterator(iter)
  }

  def flush {
    metrics.flushes.inc
    trace("Flushing.")
    db.flush(flushOptions)
  }

  def close() {
    trace("Closing.")
    db.close()
  }

  class RocksDbIterator(iter: RocksIterator) extends KeyValueIterator[Array[Byte], Array[Byte]] {
    private var open = true
    private var firstValueAccessed = false;
    def close() = {
      open = false
      iter.dispose()
    }

    def remove() = throw new UnsupportedOperationException("RocksDB iterator doesn't support remove");

    def hasNext() = iter.isValid

    // The iterator is already pointing to the next element
    protected def peekKey() = {
      getEntry().getKey
    }

    protected def getEntry() = {
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
    def next() = {
      if (!hasNext()) {
        throw new NoSuchElementException
      }

      val entry = getEntry()
      iter.next
      metrics.bytesRead.inc(entry.getKey.size)
      if (entry.getValue != null) {
        metrics.bytesRead.inc(entry.getValue.size)
      }
      entry
    }

    override def finalize() {
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
    val comparator = lexicographic
    iter.seek(from)
    override def hasNext() = {
      super.hasNext() && comparator.compare(peekKey(), to) < 0
    }
  }
}
