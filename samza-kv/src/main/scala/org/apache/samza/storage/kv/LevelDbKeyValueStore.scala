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

import java.nio.ByteBuffer
import org.iq80.leveldb._
import org.fusesource.leveldbjni.internal.NativeComparator
import org.fusesource.leveldbjni.JniDBFactory._
import java.io._
import java.util.Iterator
import java.lang.Iterable
import org.apache.samza.config.Config
import grizzled.slf4j.Logging

object LevelDbKeyValueStore {
  def options(config: Config) = {
    val options = new Options
    options.blockSize(config.getInt("block.size", 4086))
    options.cacheSize(config.getLong("cache.size", 64 * 1024 * 1024L))
    options.compressionType(if (config.getBoolean("compress", true)) CompressionType.SNAPPY else CompressionType.NONE)
    options.createIfMissing(true)
    options.errorIfExists(true)
    options.writeBufferSize(config.getInt("write.buffer.size", 16 * 1024 * 1024))
    options
  }
}

class LevelDbKeyValueStore(
  val dir: File,
  val options: Options) extends KeyValueStore[Array[Byte], Array[Byte]] with Logging {

  private lazy val db = factory.open(dir, options)
  private val lexicographic = new LexicographicComparator()

  def get(key: Array[Byte]): Array[Byte] = {
    require(key != null, "Null key not allowed.")
    val found = db.get(key)
    if (found == null)
      null
    else
      found
  }

  def put(key: Array[Byte], value: Array[Byte]) {
    require(key != null, "Null key not allowed.")
    if (value == null)
      db.delete(key)
    else
      db.put(key, value)
  }

  def putAll(entries: java.util.List[Entry[Array[Byte], Array[Byte]]]) {
    val batch = db.createWriteBatch()
    val iter = entries.iterator
    while (iter.hasNext) {
      val curr = iter.next()
      if (curr.getValue == null)
        batch.delete(curr.getKey)
      else
        batch.put(curr.getKey, curr.getValue)
    }
    db.write(batch)
  }

  def delete(key: Array[Byte]) {
    put(key, null)
  }

  def range(from: Array[Byte], to: Array[Byte]): KeyValueIterator[Array[Byte], Array[Byte]] = {
    require(from != null && to != null, "Null bound not allowed.")
    new LevelDbRangeIterator(db.iterator, from, to)
  }

  def all(): KeyValueIterator[Array[Byte], Array[Byte]] = {
    val iter = db.iterator()
    iter.seekToFirst()
    new LevelDbIterator(iter)
  }

  def flush {
    // TODO can't find a flush for leveldb
    trace("Flushing, but flush in LevelDbKeyValueStore doesn't do anything.")
  }

  def close() {
    trace("Closing.")

    db.close()
  }

  class LevelDbIterator(iter: DBIterator) extends KeyValueIterator[Array[Byte], Array[Byte]] {
    private var open = true
    def close() = {
      open = false
      iter.close()
    }
    def remove() = iter.remove()
    def hasNext() = iter.hasNext()
    def next() = {
      val curr = iter.next()
      new Entry(curr.getKey, curr.getValue)
    }
    override def finalize() {
      if (open) {
        System.err.println("Leaked reference to level db iterator, forcing close.")
        close()
      }
    }
  }

  class LevelDbRangeIterator(iter: DBIterator, from: Array[Byte], to: Array[Byte]) extends LevelDbIterator(iter) {
    val comparator = if (options.comparator == null) lexicographic else options.comparator
    iter.seek(from)
    override def hasNext() = {
      iter.hasNext() && comparator.compare(iter.peekNext.getKey, to) <= 0
    }
  }

  /**
   * Compare two array lexicographically using unsigned byte arithmetic
   */
  class LexicographicComparator extends DBComparator {
    def compare(k1: Array[Byte], k2: Array[Byte]): Int = {
      val l = math.min(k1.length, k2.length)
      var i = 0
      while (i < l) {
        if (k1(i) != k2(i))
          return (k1(i) & 0xff) - (k2(i) & 0xff)
        i += 1
      }
      // okay prefixes are equal, the shorter array is less
      k1.length - k2.length
    }
    def name(): String = "lexicographic"
    def findShortestSeparator(start: Array[Byte], limit: Array[Byte]) = start
    def findShortSuccessor(key: Array[Byte]) = key
  }

}
