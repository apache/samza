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

  object DBOperations extends Enumeration {
    type DBOperations = Int
    val READ = 1
    val WRITE = 2
    val DELETE = 3
    val RANGE = 4
  }

  val streamName = storageConfig.getAccessLogStream(changelogSystemStreamPartition.getSystemStream.getStream)
  val systemStream = new SystemStream(changelogSystemStreamPartition.getSystemStream.getSystem, streamName)
  val partitionId: Int = changelogSystemStreamPartition.getPartition.getPartitionId
  val serializer = new StringSerde("UTF-8")
  val sample = storageConfig.getAccessLogSamplingRatio(storeName)
  val rng = scala.util.Random

  def get(key: K): V = {
    val isRange = 0
    measureLatencyAndWriteToStream(DBOperations.READ, isRange, toBytesOrNull(key), null, store.get(key))
  }

  def getAll(keys: util.List[K]): util.Map[K, V] = {
    val isRange = 1
    measureLatencyAndWriteToStream(DBOperations.READ, isRange, null, toBytesKey(keys.iterator()), store.getAll(keys))
  }

  def put(key: K, value: V): Unit = {
    val isRange = 0
    measureLatencyAndWriteToStream(DBOperations.WRITE, isRange, toBytesOrNull(key), null, store.put(key, value))
  }

  def putAll(entries: util.List[Entry[K, V]]): Unit = {
    val iter = entries.iterator
    val isRange = 1
    measureLatencyAndWriteToStream(DBOperations.WRITE, isRange, null, toBytesKeyFromEntries(iter), store.putAll(entries))
  }

  def delete(key: K): Unit = {
    val isRange = 0
    measureLatencyAndWriteToStream(DBOperations.DELETE, isRange, toBytesOrNull(key), null, store.delete(key))
  }

  def deleteAll(keys: util.List[K]): Unit = {
    val isRange = 1
    measureLatencyAndWriteToStream(DBOperations.DELETE, isRange, null, toBytesKey(keys.iterator()), store.deleteAll(keys))
  }

  def range(from: K, to: K): KeyValueIterator[K, V] = {
    val isRange = 1
    val list : util.ArrayList[K] = new util.ArrayList[K]()
    list.add(from)
    list.add(to)
    measureLatencyAndWriteToStream(DBOperations.RANGE, isRange, null, toBytesKey(list.iterator()), store.range(from, to))
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


  def toBytesKey(keys: util.Iterator[K]): util.ArrayList[Array[Byte]] = {
    val keysInBytes = new util.ArrayList[Array[Byte]]
    if (keys != null)
      while(keys.hasNext()) {
        val entry = keys.next()
        keysInBytes.add(toBytesOrNull(entry))
      }

    keysInBytes
  }

  def toBytesKeyFromEntries(iter: util.Iterator[Entry[K, V]]): util.ArrayList[Array[Byte]] = {
    val keysInBytes = new util.ArrayList[Array[Byte]]
    if (iter != null)
      while(iter.hasNext()) {
        val entry = iter.next().getKey
        keysInBytes.add(toBytesOrNull(entry))
      }

    keysInBytes
  }

  private def measureLatencyAndWriteToStream[R](dBOperations: Int, isRange: Int, singleKey: Array[Byte], keys: util.ArrayList[Array[Byte]], block: => R):R = {
    val time1 = System.nanoTime()
    val result = block
    val time2 = System.nanoTime()
    if (rng.nextInt() > sample) {
      return result
    }

    val latency = time2 - time1
    val timeStamp = System.currentTimeMillis().toString
    val message = new AccessLogMessage(dBOperations, isRange, latency, singleKey, keys, timeStamp)
    collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, serializer.toBytes(timeStamp), message.serializeMessage()))
    result
  }

  def toBytesOrNull(key: K): Array[Byte] = {
    if (key == null) {
      return null
    }
    val bytes = keySerde.toBytes(key)
    return bytes
  }

}
