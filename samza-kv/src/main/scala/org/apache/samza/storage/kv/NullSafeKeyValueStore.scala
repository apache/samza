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

import org.apache.samza.util.Util.notNull

import scala.collection.JavaConversions._

object NullSafeKeyValueStore {
  val NullKeyErrorMessage = "Null is not a valid key."
  val NullKeysErrorMessage = "Null is not a valid keys list."
  val NullValueErrorMessage = "Null is not a valid value."
}

class NullSafeKeyValueStore[K, V](store: KeyValueStore[K, V]) extends KeyValueStore[K, V] {
  import NullSafeKeyValueStore._

  def get(key: K): V = {
    notNull(key, NullKeyErrorMessage)
    store.get(key)
  }

  def getAll(keys: java.util.List[K]): java.util.Map[K, V] = {
    notNull(keys, NullKeysErrorMessage)
    keys.foreach(key => notNull(key, NullKeyErrorMessage))
    store.getAll(keys)
  }

  def put(key: K, value: V) {
    notNull(key, NullKeyErrorMessage)
    notNull(value, NullValueErrorMessage)
    store.put(key, value)
  }

  def putAll(entries: java.util.List[Entry[K, V]]) {
    entries.foreach(entry => {
      notNull(entry.getKey, NullKeyErrorMessage)
      notNull(entry.getValue, NullValueErrorMessage)
    })
    store.putAll(entries)
  }

  def delete(key: K) {
    notNull(key, NullKeyErrorMessage)
    store.delete(key)
  }

  def deleteAll(keys: java.util.List[K]) = {
    notNull(keys, NullKeysErrorMessage)
    keys.foreach(key => notNull(key, NullKeyErrorMessage))
    store.deleteAll(keys)
  }

  def range(from: K, to: K): KeyValueIterator[K, V] = {
    notNull(from, NullKeyErrorMessage)
    notNull(to, NullKeyErrorMessage)
    store.range(from, to)
  }

  def all(): KeyValueIterator[K, V] = {
    store.all
  }

  def flush {
    store.flush
  }

  def close {
    store.close
  }
}
