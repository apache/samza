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
import scala.collection._
import java.util.Arrays

/**
 * A write-behind caching layer around the rocksdb store. The purpose of this cache is three-fold:
 * 1. Batch together writes to rocksdb, this turns out to be a great optimization
 * 2. Avoid duplicate writes and duplicate log entries within a commit interval. i.e. if there are two updates to the same key, log only the later.
 * 3. Avoid deserialization cost for gets on very common keys
 *
 * This caching does introduce a few odd corner cases :-(
 * 1. Items in the cache have pass-by-reference semantics but items in rocksdb have pass-by-value semantics. Modifying items after a put is a bad idea.
 * 2. Range queries require flushing the cache (as the ordering comes from rocksdb)
 *
 * In implementation this cache is just an LRU hash map that discards the oldest entry when full. There is an accompanying "dirty list" that references keys
 * that have not yet been written to disk. All writes go to the dirty list and when the list is long enough we flush out all those values at once. Dirty items
 * that time out of the cache before being written will also trigger a full flush of the dirty list.
 *
 * This class is very non-thread safe.
 *
 * @param store The store to cache
 * @param cacheSize The number of entries to hold in the in memory-cache
 * @param writeBatchSize The number of entries to batch together before forcing a write
 * @param metrics The metrics recording object for this cached store
 */
class CachedStore[K, V](
  val store: KeyValueStore[K, V],
  val cacheSize: Int,
  val writeBatchSize: Int,
  val metrics: CachedStoreMetrics = new CachedStoreMetrics) extends KeyValueStore[K, V] with Logging {

  /** the number of items in the dirty list */
  @volatile private var dirtyCount = 0

  /** the number of items currently in the cache */
  @volatile private var cacheCount = 0

  /** the list of items to be written out on flush from newest to oldest */
  private var dirty = new mutable.DoubleLinkedList[K]()

  /** an lru cache of values that holds cacheEntries and calls flush() if necessary when discarding */
  private val cache = new java.util.LinkedHashMap[K, CacheEntry[K, V]]((cacheSize * 1.2).toInt, 1.0f, true) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[K, CacheEntry[K, V]]): Boolean = {
      val evict = super.size > cacheSize
      // We need backwards compatibility with the previous broken flushing behavior for array keys.
      if (evict || hasArrayKeys) {
        val entry = eldest.getValue
        // if this entry hasn't been written out yet, flush it and all other dirty keys
        if (entry.dirty != null) {
          debug("Found a dirty entry. Flushing.")

          flush()
        }
      }
      evict
    }
  }

  /** tracks whether an array has been used as a key. since this is dangerous with LinkedHashMap, we want to warn on it. **/
  private var containsArrayKeys = false

  // Use counters here, rather than directly accessing variables using .size
  // since metrics can be accessed in other threads, and cache.size is not
  // thread safe since we're using a LinkedHashMap, and dirty.size is slow
  // since it requires a full traversal of the linked list.
  metrics.setDirtyCount(() => dirtyCount)
  metrics.setCacheSize(() => cacheCount)

  override def get(key: K) = {
    metrics.gets.inc

    val c = cache.get(key)
    if (c != null) {
      metrics.cacheHits.inc
      c.value
    } else {
      val v = store.get(key)
      cache.put(key, new CacheEntry(v, null))
      cacheCount = cache.size
      v
    }
  }

  private class CachedStoreIterator(val iter: KeyValueIterator[K, V])
    extends KeyValueIterator[K, V] {

    var last: Entry[K, V] = null

    override def close(): Unit = iter.close()

    override def remove(): Unit = {
      iter.remove()
      delete(last.getKey)
    }

    override def next() = {
      last = iter.next()
      last
    }

    override def hasNext: Boolean = iter.hasNext
  }

  override def range(from: K, to: K): KeyValueIterator[K, V] = {
    metrics.ranges.inc
    flush()

    new CachedStoreIterator(store.range(from, to))
  }

  override def all(): KeyValueIterator[K, V] = {
    metrics.alls.inc
    flush()

    new CachedStoreIterator(store.all())
  }

  override def put(key: K, value: V) {
    metrics.puts.inc

    checkKeyIsArray(key)

    // Add the key to the front of the dirty list (and remove any prior
    // occurrences to dedupe).
    val found = cache.get(key)
    if (found == null || found.dirty == null) {
      this.dirtyCount += 1
    } else {
      // If we are removing the head of the list, move the head to the next
      // element. See SAMZA-45 for details.
      if (found.dirty.prev == null) {
        this.dirty = found.dirty.next
        this.dirty.prev = null
      } else {
        found.dirty.remove()
      }
    }
    this.dirty = new mutable.DoubleLinkedList(key, this.dirty)

    // Add the key to the cache (but don't allocate a new cache entry if we
    // already have one).
    if (found == null) {
      cache.put(key, new CacheEntry(value, this.dirty))
      cacheCount = cache.size
    } else {
      found.value = value
      found.dirty = this.dirty
    }

    // Flush the dirty values if the write list is full.
    if (dirtyCount >= writeBatchSize) {
      debug("Dirty count %s >= write batch size %s. Flushing." format (dirtyCount, writeBatchSize))

      flush()
    }
  }

  override def flush() {
    trace("Flushing.")

    metrics.flushes.inc

    // write out the contents of the dirty list oldest first
    val batch = new Array[Entry[K, V]](this.dirtyCount)
    var pos : Int = this.dirtyCount - 1
    for (k <- this.dirty) {
      val entry = this.cache.get(k)
      entry.dirty = null // not dirty any more
      batch(pos) = new Entry(k, entry.value)
      pos -= 1
    }
    store.putAll(Arrays.asList(batch : _*))
    store.flush()
    metrics.flushBatchSize.inc(batch.size)

    // reset the dirty list
    this.dirty = new mutable.DoubleLinkedList[K]()
    this.dirtyCount = 0
  }

  override def putAll(entries: java.util.List[Entry[K, V]]) {
    val iter = entries.iterator
    while (iter.hasNext) {
      val curr = iter.next
      put(curr.getKey, curr.getValue)
    }
  }

  override def delete(key: K) {
    metrics.deletes.inc
    put(key, null.asInstanceOf[V])
  }

  override def close() {
    trace("Closing.")
    flush()
    store.close()
  }

  override def deleteAll(keys: java.util.List[K]) = {
    KeyValueStore.Extension.deleteAll(this, keys)
  }

  private def checkKeyIsArray(key: K) {
    if (!containsArrayKeys && key.isInstanceOf[Array[_]]) {
      // Warn the first time that we see an array key.
      warn("Using arrays as keys results in unpredictable behavior since cache is implemented with a map. Consider using ByteBuffer, or a different key type.")
      containsArrayKeys = true
    }
  }

  override def getAll(keys: java.util.List[K]): java.util.Map[K, V] = {
    metrics.gets.inc(keys.size)
    val returnValue = new java.util.HashMap[K, V](keys.size)
    val misses = new java.util.ArrayList[K]
    val keysIterator = keys.iterator
    while (keysIterator.hasNext) {
      val key = keysIterator.next
      val cached = cache.get(key)
      if (cached != null) {
        metrics.cacheHits.inc
        returnValue.put(key, cached.value)
      } else {
        misses.add(key)
      }
    }
    if (!misses.isEmpty) {
      val entryIterator = store.getAll(misses).entrySet.iterator
      while (entryIterator.hasNext) {
        val entry = entryIterator.next
        returnValue.put(entry.getKey, entry.getValue)
        cache.put(entry.getKey, new CacheEntry(entry.getValue, null))
      }
      cacheCount = cache.size // update outside the loop since it's used for metrics and not for time-sensitive logic
    }
    returnValue
  }

  def hasArrayKeys = containsArrayKeys
}

private case class CacheEntry[K, V](var value: V, var dirty: mutable.DoubleLinkedList[K])
