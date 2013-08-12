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
import org.apache.samza.system.SystemStream
import org.apache.samza.task.MessageCollector
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemStreamPartition
import grizzled.slf4j.Logging

/**
 * A key/value store decorator that adds a changelog for any changes made to the underlying store
 */
class LoggedStore[K,V](val store: KeyValueStore[K, V],
                       val systemStreamPartition: SystemStreamPartition,
                       val collector: MessageCollector) extends KeyValueStore[K, V] with Logging {

  val systemStream = systemStreamPartition.getSystemStream
  val partitionId = systemStreamPartition.getPartition.getPartitionId

  /* pass through methods */
  def get(key: K) = store.get(key)
  def range(from: K, to: K) = store.range(from, to)
  def all() = store.all()
  
  /**
   * Perform the local update and log it out to the changelog
   */
  def put(key: K, value: V) {
    store.put(key, value)
    collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, key, value))
  }
  
  /**
   * Perform multiple local updates and log out all changes to the changelog
   */
  def putAll(entries: java.util.List[Entry[K,V]]) {
    store.putAll(entries)
    val iter = entries.iterator
    while(iter.hasNext) {
      val curr = iter.next
      collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, curr.getKey, curr.getValue))
    }
  }
  
  /**
   * Perform the local delete and log it out to the changelog 
   */
  def delete(key: K) {
    store.delete(key)
    collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, key, null))
  }
  
  def flush {
    trace("Flushing.")

    store.flush
  }
  
  def close {
    trace("Closing.")

    store.close
  }
  
}
