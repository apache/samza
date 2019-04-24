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

import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.util.Logging
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemStreamPartition}
import org.apache.samza.task.MessageCollector

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * A key/value store decorator that adds a changelog for any changes made to the underlying store
 */
class LoggedStore(
  val store: KeyValueStore[Array[Byte], Array[Byte]],
  val storeName: String,
  val storeConfig: Config,
  val systemStreamPartition: SystemStreamPartition,
  val collector: MessageCollector,
  val metrics: LoggedStoreMetrics = new LoggedStoreMetrics) extends KeyValueStore[Array[Byte], Array[Byte]] with Logging {

  val systemStream = systemStreamPartition.getSystemStream
  val partitionId = systemStreamPartition.getPartition.getPartitionId

  private val DEFAULT_CHANGELOG_MAX_MSG_SIZE_BYTES = 1000000
  private val CHANGELOG_MAX_MSG_SIZE_BYTES = "changelog.max.message.size.bytes"
  private val maxMessageSize = storeConfig.getInt(CHANGELOG_MAX_MSG_SIZE_BYTES, DEFAULT_CHANGELOG_MAX_MSG_SIZE_BYTES) // slightly less than 1 MB

  /* pass through methods */
  def get(key: Array[Byte]) = {
    metrics.gets.inc
    store.get(key)
  }

  override def getAll(keys: java.util.List[Array[Byte]]): java.util.Map[Array[Byte], Array[Byte]] = {
    metrics.gets.inc(keys.size)
    store.getAll(keys)
  }

  def range(from: Array[Byte], to: Array[Byte]) = {
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
  def put(key: Array[Byte], value: Array[Byte]) {
    validateMessageSize(key, value)
    metrics.puts.inc
    collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, key, value))
    store.put(key, value)
  }

  /**
   * Perform multiple local updates and log out all changes to the changelog
   */
  def putAll(entries: java.util.List[Entry[Array[Byte], Array[Byte]]]) {
    entries.asScala.foreach(entry => {
      validateMessageSize(entry.getKey, entry.getValue)
    })
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
  def delete(key: Array[Byte]) {
    metrics.deletes.inc
    collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, key, null))
    store.delete(key)
  }

  /**
   * Perform the local deletes and log them out to the changelog
   */
  override def deleteAll(keys: java.util.List[Array[Byte]]) = {
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

  override def snapshot(from: Array[Byte], to: Array[Byte]): KeyValueSnapshot[Array[Byte], Array[Byte]] = {
    store.snapshot(from, to)
  }

  private def validateMessageSize(key: Array[Byte], message: Array[Byte]): Unit = {
    if (message.length > maxMessageSize) {
      throw new SamzaException("RocksDB message size " + message.length + " for store " + storeName
        + " was larger than the maximum allowed message size " + maxMessageSize + ".")
    }
  }
}
