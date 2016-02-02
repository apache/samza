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

import org.junit.Test
import org.junit.Assert._
import org.mockito.Mockito._

import java.util.Arrays

class TestCachedStore {
  @Test
  def testArrayCheck() {
    val kv = mock(classOf[KeyValueStore[Array[Byte], Array[Byte]]])
    val store = new CachedStore[Array[Byte], Array[Byte]](kv, 100, 100)

    assertFalse(store.hasArrayKeys)
    store.put("test1-key".getBytes("UTF-8"), "test1-value".getBytes("UTF-8"))
    // Ensure we preserve old, broken flushing behavior for array keys
    verify(kv).flush();
    assertTrue(store.hasArrayKeys)
  }

  @Test
  def testIterator() {
    val kv = new MockKeyValueStore()
    val store = new CachedStore[String, String](kv, 100, 100)

    val keys = Arrays.asList("test1-key",
                             "test2-key",
                             "test3-key")
    val values = Arrays.asList("test1-value",
                               "test2-value",
                               "test3-value")

    for (i <- 0 until 3) {
      store.put(keys.get(i), values.get(i))
    }

    // test all iterator
    var iter = store.all()
    for (i <- 0 until 3) {
      assertTrue(iter.hasNext)
      val entry = iter.next()
      assertEquals(entry.getKey, keys.get(i))
      assertEquals(entry.getValue, values.get(i))
    }
    assertFalse(iter.hasNext)

    // test range iterator
    iter = store.range(keys.get(0), keys.get(2))
    for (i <- 0 until 2) {
      assertTrue(iter.hasNext)
      val entry = iter.next()
      assertEquals(entry.getKey, keys.get(i))
      assertEquals(entry.getValue, values.get(i))
    }
    assertFalse(iter.hasNext)

    // test iterator remove
    iter = store.all()
    iter.next()
    iter.remove()

    assertNull(kv.get(keys.get(0)))
    assertNull(store.get(keys.get(0)))

    iter = store.range(keys.get(1), keys.get(2))
    iter.next()
    iter.remove()

    assertFalse(iter.hasNext)
    assertNull(kv.get(keys.get(1)))
    assertNull(store.get(keys.get(1)))
  }

  @Test
  def testFlushing() {
    val kv = mock(classOf[KeyValueStore[String, String]])
    val store = new CachedStore[String, String](kv, 4, 4)

    val keys = Arrays.asList("test1-key",
      "test2-key",
      "test3-key",
      "test4-key")
    val values = Arrays.asList("test1-value",
      "test2-value",
      "test3-value",
      "test4-value")

    for (i <- 0 until 3) {
      store.put(keys.get(i), values.get(i))
    }

    verify(kv, never()).flush()
    store.put(keys.get(3), values.get(3));
    verify(kv).flush();
  }
}
