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

import scala.collection._
import grizzled.slf4j.Logging

/**
 * A write-behind caching layer around the leveldb store. The purpose of this cache is three-fold:
 * 1. Batch together writes to leveldb, this turns out to be a great optimization
 * 2. Avoid duplicate writes and duplicate log entries within a commit interval. i.e. if there are two updates to the same key, log only the later.
 * 3. Avoid deserialization cost for gets on very common keys
 *
 * This caching does introduce a few odd corner cases :-(
 * 1. Items in the cache have pass-by-reference semantics but items in leveldb have pass-by-value semantics. Modifying items after a put is a bad idea.
 * 2. Range queries require flushing the cache (as the ordering comes from leveldb)
 *
 * In implementation this cache is just an LRU hash map that discards the oldest entry when full. There is an accompanying "dirty list" that references keys
 * that have not yet been written to disk. All writes go to the dirty list and when the list is long enough we flush out all those values at once. Dirty items
 * that time out of the cache before being written will also trigger a full flush of the dirty list.
 *
 * This class is very non-thread safe.
 *
 * @param store The store to cache
 * @param cacheEntries The number of entries to hold in the in memory-cache
 * @param writeBatchSize The number of entries to batch together before forcing a write
 */
class CachedStore[K, V](val store: KeyValueStore[K, V],
  val cacheSize: Int,
  val writeBatchSize: Int) extends KeyValueStore[K, V] with Logging {

  /** the number of items in the dirty list */
  private var dirtyCount = 0

  /** the list of items to be written out on flush from newest to oldest */
  private var dirty = new mutable.DoubleLinkedList[K]()

  /** an lru cache of values that holds cacheEntries and calls flush() if necessary when discarding */
  private val cache = new java.util.LinkedHashMap[K, CacheEntry[K, V]]((cacheSize * 1.2).toInt, 1.0f, true) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[K, CacheEntry[K, V]]): Boolean = {
      val entry = eldest.getValue
      // if this entry hasn't been written out yet, flush it and all other dirty keys
      if (entry.dirty != null) {
        debug("Found a dirty entry. Flushing.")

        flush()
      }
      super.size > cacheSize
    }
  }

  def get(key: K) = {
    val c = cache.get(key)
    if (c != null) {
      c.value
    } else {
      val v = store.get(key)
      cache.put(key, new CacheEntry(v, null))
      v
    }
  }

  def range(from: K, to: K) = {
    flush()
    store.range(from, to)
  }

  def all() = {
    flush()
    store.all()
  }

  def put(key: K, value: V) {
    // add the key to the front of the dirty list (and remove any prior occurrences to dedupe)
    val found = cache.get(key)
    if (found == null || found.dirty == null)
      this.dirtyCount += 1
    else
      found.dirty.remove()
    this.dirty = new mutable.DoubleLinkedList(key, this.dirty)

    // add the key to the cache (but don't allocate a new cache entry if we already have one)
    if (found == null) {
      cache.put(key, new CacheEntry(value, this.dirty))
    } else {
      found.value = value
      found.dirty = this.dirty
    }

    // flush the dirty values if the write list is full
    if (dirtyCount >= writeBatchSize) {
      debug("Dirty count %s >= write batch size %s. Flushing." format (dirtyCount, writeBatchSize))

      flush()
    }
  }

  def flush() {
    trace("Flushing.")

    // write out the contents of the dirty list oldest first
    val batch = new java.util.ArrayList[Entry[K, V]](this.dirtyCount)
    for (k <- this.dirty.reverse) {
      val entry = this.cache.get(k)
      entry.dirty = null // not dirty any more
      batch.add(new Entry(k, entry.value))
    }
    store.putAll(batch)
    store.flush

    // reset the dirty list
    this.dirty = new mutable.DoubleLinkedList[K]()
    this.dirtyCount = 0
  }

  /**
   * Perform multiple local updates and log out all changes to the changelog
   */
  def putAll(entries: java.util.List[Entry[K, V]]) {
    val iter = entries.iterator
    while (iter.hasNext) {
      val curr = iter.next
      put(curr.getKey, curr.getValue)
    }
  }

  /**
   * Perform the local delete and log it out to the changelog
   */
  def delete(key: K) {
    put(key, null.asInstanceOf[V])
  }

  def close() {
    trace("Closing.")

    flush
    
    store.close
  }

}

private case class CacheEntry[K, V](var value: V, var dirty: mutable.DoubleLinkedList[K])
