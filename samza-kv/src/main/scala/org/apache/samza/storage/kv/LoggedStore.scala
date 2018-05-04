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
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemStreamPartition}
import org.apache.samza.task.MessageCollector

/**
 * A key/value store decorator that adds a changelog for any changes made to the underlying store
 */
class LoggedStore[K, V](
  val store: KeyValueStore[K, V],
  val systemStreamPartition: SystemStreamPartition,
  val collector: MessageCollector,
  val metrics: LoggedStoreMetrics = new LoggedStoreMetrics) extends KeyValueStore[K, V] with Logging {

  val systemStream = systemStreamPartition.getSystemStream
  val partitionId = systemStreamPartition.getPartition.getPartitionId

  /* pass through methods */
  def get(key: K) = {
    metrics.gets.inc
    store.get(key)
  }

  override def getAll(keys: java.util.List[K]): java.util.Map[K, V] = {
    metrics.gets.inc(keys.size)
    store.getAll(keys)
  }

  def range(from: K, to: K) = {
    metrics.ranges.inc
    store.range(from, to)
  }

  def all() = {
    metrics.alls.inc
    store.all()
  }

  /**
   * Perform the local update and log it out to the changelog
   */
  def put(key: K, value: V) {
    metrics.puts.inc
    collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, key, value))
    store.put(key, value)
  }

  /**
   * Perform multiple local updates and log out all changes to the changelog
   */
  def putAll(entries: java.util.List[Entry[K, V]]) {
    metrics.puts.inc(entries.size)
    val iter = entries.iterator
    while (iter.hasNext) {
      val curr = iter.next
      collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, curr.getKey, curr.getValue))
    }
    store.putAll(entries)
  }

  /**
   * Perform the local delete and log it out to the changelog
   */
  def delete(key: K) {
    metrics.deletes.inc
    collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, key, null))
    store.delete(key)
  }

  /**
   * Perform the local deletes and log them out to the changelog
   */
  override def deleteAll(keys: java.util.List[K]) = {
    metrics.deletes.inc(keys.size)
    val keysIterator = keys.iterator
    while (keysIterator.hasNext) {
      collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, keysIterator.next, null))
    }
    store.deleteAll(keys)
  }

  def flush {
    trace("Flushing store.")

    metrics.flushes.inc

    store.flush
    trace("Flushed store.")
  }

  def close {
    trace("Closing.")

    store.close
  }

  override def iterate(from: K, to: K): KeyValueIterable[K, V] = {
    store.iterate(from, to)
  }
}
