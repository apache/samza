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
    val systemStreamPartition: SystemStreamPartition,
    val storageConfig: StorageConfig,
    val storeName: String) extends KeyValueStore[K, V] with Logging {

  object DBOperations extends Enumeration {
    type DBOperations = Value
    val READ = Value("read")
    val WRITE = Value("write")
    val DELETE = Value("delete")
    val RANGE = Value("range")
    val READ_ALL = Value("read_all")
    val WRITE_ALL = Value("write_all")
    val DELETE_ALL = Value("delete_all")
  }

  val streamName = storageConfig.getAccessLogStream(systemStreamPartition.getSystemStream.getStream)
  val systemStream = new SystemStream(systemStreamPartition.getSystemStream.getSystem, streamName)
  val partitionId: Int = systemStreamPartition.getPartition.getPartitionId
  val serializer = new StringSerde("UTF-8")
  val sample = storageConfig.getSamplingSetting(storeName)
  val generator = scala.util.Random
  val DELIMITER = " || "

  def get(key: K): V = {
    var message = DBOperations.READ + DELIMITER + key
    measureLatencyAndWriteToStream(message, store.get(key))
  }

  def getAll(keys: util.List[K]): util.Map[K, V] = {

    var message = DBOperations.READ_ALL + DELIMITER + toStringKey(keys.iterator())
    measureLatencyAndWriteToStream(message, store.getAll(keys))
  }

  def put(key: K, value: V): Unit = {
    var message = DBOperations.WRITE + DELIMITER  + key
    measureLatencyAndWriteToStream(message, store.put(key, value))
  }

  def putAll(entries: util.List[Entry[K, V]]): Unit = {
    val iter = entries.iterator
    var message = DBOperations.WRITE_ALL + DELIMITER  + toStringEntry(iter)
    measureLatencyAndWriteToStream(message, store.putAll(entries))
  }


  def delete(key: K): Unit = {
    var message = DBOperations.DELETE  + DELIMITER + key
    measureLatencyAndWriteToStream(message, store.delete(key))
  }

  def deleteAll(keys: util.List[K]): Unit = {
    var message = DBOperations.DELETE_ALL + DELIMITER + toStringKey(keys.iterator())
    measureLatencyAndWriteToStream(message, store.deleteAll(keys))
  }

  def range(from: K, to: K): KeyValueIterator[K, V] = {
    var message = DBOperations.RANGE + DELIMITER +  "(" + from + "," + to + ")"
    measureLatencyAndWriteToStream(message, store.range(from, to))
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


  def toStringKey(list: util.Iterator[K]): String = {
    var serializedValue = "( "
    while(list.hasNext) {
      serializedValue += list.next() + ", "
    }
    serializedValue += ") "
    serializedValue
  }

  def toStringEntry(iter: util.Iterator[Entry[K, V]]): String = {
    var serializedValue = "( "
    while(iter.hasNext) {
      val entry = iter.next()
      serializedValue += entry.getKey + ":" + entry.getValue + ", "
    }

    serializedValue += ") "
    serializedValue
  }

  private def measureLatencyAndWriteToStream[R](message: String, block: => R):R = {
    val time1 = System.nanoTime()
    val result = block
    val time2 = System.nanoTime()
    if (generator.nextInt() > sample) {
      return result
    }

    val latency = time2 - time1
    var msg = message
    val timeStamp = System.nanoTime().toString

    msg += DELIMITER + latency + DELIMITER + timeStamp
    collector.send(new OutgoingMessageEnvelope(systemStream, partitionId, serializer.toBytes(timeStamp), serializer.toBytes(msg)))
    result
  }

}
