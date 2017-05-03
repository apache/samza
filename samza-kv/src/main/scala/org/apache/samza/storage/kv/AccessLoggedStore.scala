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


import java.util
import org.apache.samza.config.StorageConfig
import org.apache.samza.task.MessageCollector
import org.apache.samza.util.Logging
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemStream, SystemStreamPartition}
import org.apache.samza.serializers._

class AccessLoggedStore[K, V](
    val store: KeyValueStore[K, V],
    val collector: MessageCollector,
    val changelogSystemStreamPartition: SystemStreamPartition,
    val storageConfig: StorageConfig,
    val storeName: String,
    val keySerde: Serde[K]) extends KeyValueStore[K, V] with Logging {

  object DBOperation extends Enumeration {
    type DBOperation = Int
    val READ = 1
    val WRITE = 2
    val DELETE = 3
    val RANGE = 4
  }

  val streamName = storageConfig.getAccessLogStream(changelogSystemStreamPartition.getSystemStream.getStream)
  val systemStream = new SystemStream(changelogSystemStreamPartition.getSystemStream.getSystem, streamName)
  val partitionId: Int = changelogSystemStreamPartition.getPartition.getPartitionId
  val serializer = new LongSerde()
  val samplingRatio = storageConfig.getAccessLogSamplingRatio(storeName)
  val rng = scala.util.Random

  def get(key: K): V = {
    val list = new util.ArrayList[Array[Byte]]
    list.add(toBytesOrNull(key))
    logAccess(DBOperation.READ, list, store.get(key))
  }

  def getAll(keys: util.List[K]): util.Map[K, V] = {
    logAccess(DBOperation.READ, toBytesKey(keys), store.getAll(keys))
  }

  def put(key: K, value: V): Unit = {
    val list = new util.ArrayList[Array[Byte]]
    list.add(toBytesOrNull(key))
    logAccess(DBOperation.WRITE, list, store.put(key, value))
  }

  def putAll(entries: util.List[Entry[K, V]]): Unit = {
    logAccess(DBOperation.WRITE, toBytesKeyFromEntries(entries), store.putAll(entries))
  }

  def delete(key: K): Unit = {
    val list = new util.ArrayList[Array[Byte]]
    list.add(toBytesOrNull(key))
    logAccess(DBOperation.DELETE, list, store.delete(key))
  }

  def deleteAll(keys: util.List[K]): Unit = {
    logAccess(DBOperation.DELETE, toBytesKey(keys), store.deleteAll(keys))
  }

  def range(from: K, to: K): KeyValueIterator[K, V] = {
    val list : util.ArrayList[K] = new util.ArrayList[K]()
    list.add(from)
    list.add(to)
    logAccess(DBOperation.RANGE, toBytesKey(list), store.range(from, to))
  }

  def all(): KeyValueIterator[K, V] = {
    store.all()
  }

  def close(): Unit = {
    trace("Closing.")

    store.close
  }

  def flush(): Unit = {
    trace("Flushing store.")

    store.flush
    trace("Flushed store.")
  }


  def toBytesKey(keys: util.List[K]): util.ArrayList[Array[Byte]] = {
    val keysInBytes = new util.ArrayList[Array[Byte]]
    val iter = keys.iterator
    if (iter != null)
      while(iter.hasNext()) {
        val entry = iter.next()
        keysInBytes.add(toBytesOrNull(entry))
      }

    keysInBytes
  }

  def toBytesKeyFromEntries(list: util.List[Entry[K, V]]): util.ArrayList[Array[Byte]] = {
    val keysInBytes = new util.ArrayList[Array[Byte]]
    val iter = list.iterator
    if (iter != null)
      while(iter.hasNext()) {
        val entry = iter.next().getKey
        keysInBytes.add(toBytesOrNull(entry))
      }

    keysInBytes
  }

  private def logAccess[R](dBOperation: Int, keys: util.ArrayList[Array[Byte]],
                                                block: => R):R = {
    val startTimeNs = System.nanoTime()
    val result = block
    val endTimeNs = System.nanoTime()
    if (rng.nextInt() < samplingRatio) {
      val duration = endTimeNs - startTimeNs
      val timeStamp = System.currentTimeMillis()
      val message = new AccessLogMessage(dBOperation, duration, keys, timeStamp)
      collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, serializer.toBytes(timeStamp), message.serialize()))
    }

    result
  }

  def toBytesOrNull(key: K): Array[Byte] = {
    if (key == null) {
      return null
    }
    val bytes = keySerde.toBytes(key)
    bytes
  }

}
