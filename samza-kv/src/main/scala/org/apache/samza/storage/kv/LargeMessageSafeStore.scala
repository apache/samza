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

import org.apache.samza.SamzaException

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
  * A wrapper over KeyValueStore that checks to see if any large messages have been passed to the store.
  *
  * @param store the store to persist to if messages are not large
  * @param storeName store name
  * @param dropLargeMessage a flag that determines if we want to ignore and drop large messages
  * @param maxMessageSize maximum allowed size of a message, after which every message will be considered large
  */
class LargeMessageSafeStore(
  store: KeyValueStore[Array[Byte], Array[Byte]],
  storeName: String,
  dropLargeMessage: Boolean,
  maxMessageSize: Int) extends KeyValueStore[Array[Byte], Array[Byte]] {

  override def get(key: Array[Byte]): Array[Byte] = {
    store.get(key)
  }

  override def put(key: Array[Byte], value: Array[Byte]): Unit = {
    validateMessageSize(value)
    if (!isLargeMessage(value)) {
      store.put(key, value)
    }
  }

  override def putAll(entries: util.List[Entry[Array[Byte], Array[Byte]]]): Unit = {
    entries.asScala.foreach(entry => {
      validateMessageSize(entry.getValue)
    })
    val largeMessageSafeEntries = removeLargeMessage(entries)
    store.putAll(largeMessageSafeEntries)
  }

  override def delete(key: Array[Byte]): Unit = {
    store.delete(key)
  }

  override def deleteAll(keys: java.util.List[Array[Byte]]): Unit = {
    store.deleteAll(keys)
  }

  override def range(from: Array[Byte], to: Array[Byte]): KeyValueIterator[Array[Byte], Array[Byte]] = {
    store.range(from, to)
  }

  override def snapshot(from: Array[Byte], to: Array[Byte]): KeyValueSnapshot[Array[Byte], Array[Byte]] = {
    store.snapshot(from, to)
  }

  override def all(): KeyValueIterator[Array[Byte], Array[Byte]] = {
    store.all()
  }

  override def close(): Unit = {
    store.close()
  }

  override def flush(): Unit = {
    store.flush()
  }

  private def validateMessageSize(message: Array[Byte]): Unit = {
    if (!dropLargeMessage && isLargeMessage(message)) {
      throw new SamzaException("Logged store message size " + message.length + " for store " + storeName
        + " was larger than the maximum allowed message size " + maxMessageSize + ".")
    }
  }

  private def isLargeMessage(message:Array[Byte]): Boolean = {
    message != null && message.length > maxMessageSize
  }

  private def removeLargeMessage(entries: util.List[Entry[Array[Byte], Array[Byte]]]): util.List[Entry[Array[Byte], Array[Byte]]] = {
    val largeMessageSafeEntries = new util.ArrayList[Entry[Array[Byte], Array[Byte]]]
    entries.asScala.foreach(entry => {
      if (!isLargeMessage(entry.getValue)) {
        largeMessageSafeEntries.add(entry)
      }
    })
    largeMessageSafeEntries
  }
}
