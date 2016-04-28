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

import scala.collection.JavaConversions._
import java.util

/**
 * A mock key-value store wrapper that handles serialization
 */
class MockKeyValueStore extends KeyValueStore[String, String] {

  val kvMap = new java.util.TreeMap[String, String]()

  override def get(key: String) = kvMap.get(key)

  override def put(key: String, value: String) {
    kvMap.put(key, value)
  }

  override def putAll(entries: java.util.List[Entry[String, String]]) {
    for (entry <- entries) {
      kvMap.put(entry.getKey, entry.getValue)
    }
  }

  override def delete(key: String) {
    kvMap.remove(key)
  }

  private class MockIterator(val iter: util.Iterator[util.Map.Entry[String, String]])
    extends KeyValueIterator[String, String] {

    override def hasNext = iter.hasNext

    override def next() = {
      val entry = iter.next()
      new Entry(entry.getKey, entry.getValue)
    }

    override def remove(): Unit = iter.remove()

    override def close(): Unit = Unit
  }

  override def range(from: String, to: String): KeyValueIterator[String, String] =
    new MockIterator(kvMap.subMap(from, to).entrySet().iterator())

  override def all(): KeyValueIterator[String, String] =
    new MockIterator(kvMap.entrySet().iterator())

  override def flush() {}  // no-op

  override def close() { kvMap.clear() }

  override def deleteAll(keys: java.util.List[String]) {
    KeyValueStore.Extension.deleteAll(this, keys)
  }

  override def getAll(keys: java.util.List[String]): java.util.Map[String, String] = {
    KeyValueStore.Extension.getAll(this, keys)
  }
}