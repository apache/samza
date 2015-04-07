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
import org.apache.samza.util.{ LexicographicComparator, Logging }
import org.apache.samza.config.Config
import org.apache.samza.container.SamzaContainerContext
import org.rocksdb._

object RocksDbKeyValueStore extends Logging {
  def options(storeConfig: Config, containerContext: SamzaContainerContext) = {
    val cacheSize = storeConfig.getLong("container.cache.size.bytes", 100 * 1024 * 1024L)
    val writeBufSize = storeConfig.getLong("container.write.buffer.size.bytes", 32 * 1024 * 1024)
    val options = new Options()

    // Cache size and write buffer size are specified on a per-container basis.
    val numTasks = containerContext.taskNames.size
    options.setWriteBufferSize((writeBufSize / numTasks).toInt)
    var cacheSizePerContainer = cacheSize / numTasks
    options.setCompressionType(
      storeConfig.get("rocksdb.compression", "snappy") match {
        case "snappy" => CompressionType.SNAPPY_COMPRESSION
        case "bzip2" => CompressionType.BZLIB2_COMPRESSION
        case "zlib" => CompressionType.ZLIB_COMPRESSION
        case "lz4" => CompressionType.LZ4_COMPRESSION
        case "lz4hc" => CompressionType.LZ4HC_COMPRESSION
        case "none" => CompressionType.NO_COMPRESSION
        case _ =>
          warn("Unknown rocksdb.compression codec %s, defaulting to Snappy" format storeConfig.get("rocksdb.compression", "snappy"))
          CompressionType.SNAPPY_COMPRESSION
      })

    val blockSize = storeConfig.getInt("rocksdb.block.size.bytes", 4096)
    val bloomBits = storeConfig.getInt("rocksdb.bloomfilter.bits", 10)
    val table_options = new BlockBasedTableConfig()
    table_options.setBlockCacheSize(cacheSizePerContainer)
      .setBlockSize(blockSize)
      .setFilterBitsPerKey(bloomBits)

    options.setTableFormatConfig(table_options)
    options.setCompactionStyle(
      storeConfig.get("rocksdb.compaction.style", "universal") match {
        case "universal" => CompactionStyle.UNIVERSAL
        case "fifo" => CompactionStyle.FIFO
        case "level" => CompactionStyle.LEVEL
        case _ =>
          warn("Unknown rocksdb.compactionStyle %s, defaulting to universal" format storeConfig.get("rocksdb.compaction.style", "universal"))
          CompactionStyle.UNIVERSAL
      })

    options.setMaxWriteBufferNumber(storeConfig.get("rocksdb.num.write.buffers", "3").toInt)
    options.setCreateIfMissing(true)
    options.setErrorIfExists(true)
    options
  }

}

class RocksDbKeyValueStore(
  val dir: File,
  val options: Options,
  val writeOptions: WriteOptions = new WriteOptions(),
  val metrics: KeyValueStoreMetrics = new KeyValueStoreMetrics) extends KeyValueStore[Array[Byte], Array[Byte]] with Logging {

  private lazy val db = RocksDB.open(options, dir.toString)
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
    // TODO still not exposed in Java RocksDB API, follow up with rocksDB team
    trace("Flush in RocksDbKeyValueStore is not supported, ignoring")
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
        trace("Leaked reference to level db iterator, forcing close.")
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