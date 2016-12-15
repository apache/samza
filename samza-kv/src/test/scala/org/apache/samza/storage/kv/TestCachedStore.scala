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
import java.util.Arrays

import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.anyObject
import org.mockito.Mockito._

import scala.collection.JavaConverters._

class TestCachedStore {
  @Test
  def testArrayCheck() {
    val kv = mock(classOf[KeyValueStore[Array[Byte], Array[Byte]]])
    val store = new CachedStore[Array[Byte], Array[Byte]](kv, 100, 100)

    assertFalse(store.hasArrayKeys)
    store.put("test1-key".getBytes("UTF-8"), "test1-value".getBytes("UTF-8"))
    assertTrue(store.hasArrayKeys)
  }

  @Test
  def testLRUCacheEviction() {
    val kv = spy(new MockKeyValueStore())
    val store = new CachedStore[String, String](kv, 2, 2)
    assertFalse("KV store should be empty", kv.all().hasNext)

    // Below eviction threshold
    store.put("test1-key", "test1-value")
    assertFalse("Entries should not have been purged yet", kv.all().hasNext)

    // Batch limit reached
    store.put("test2-key", "test2-value")
    assertTrue("Entries should be purged as soon as there are batchSize dirty entries", kv.all().hasNext)

    // kv.putAll() should have been called, verified below.

    // All dirty values should have been added to the underlying store
    // KV store should have both items
    val kvItr = kv.all();
    assertNotNull(kvItr.next())
    assertNotNull(kvItr.next())
    assertFalse(kvItr.hasNext)

    // Above eviction threshold but eldest entries are not dirty
    store.put("test3-key", "test3-value")

    // KV store should not have the 3rd item. We only purge if the batch size is exceeded or if the eldest(expiring) entry is dirty.
    val kvItr2 = kv.all();
    assertNotNull(kvItr2.next())
    assertNotNull(kvItr2.next())
    assertFalse(kvItr2.hasNext)

    // Force the dirty key to be the eldest by reading a different key
    store.get("test2-key")

    // Add one more. We should not purge all items again. Only when dirty items exceed the threshold.
    store.put("test4-key", "test4-value")

    // The eldest item should have been purged along with the just-added item, so the KV store should have all 4 items.
    val kvItr3 = kv.all();
    assertNotNull(kvItr3.next())
    assertNotNull(kvItr3.next())
    assertNotNull(kvItr3.next())
    assertNotNull(kvItr3.next())
    assertFalse(kvItr3.hasNext)

    // There should have been 2 purges; one for exceeding the batch size, and one for expiring a dirty cache entry.
    verify(kv, times(2)).putAll(anyObject());
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

  }

  @Test
  def testPutAllDirtyEntries() {
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

    verify(kv, never()).putAll(anyObject())
    verify(kv, never()).flush()
    store.put(keys.get(3), values.get(3))

    val entriesCaptor = ArgumentCaptor.forClass(classOf[util.List[Entry[String, String]]])
    verify(kv).putAll(entriesCaptor.capture)
    verify(kv, never()).flush()

    val dirtyEntries = entriesCaptor.getAllValues.get(0).asScala.toSeq
    assertEquals(dirtyEntries map (_.getKey), Seq("test1-key", "test2-key", "test3-key", "test4-key"))
    assertEquals(dirtyEntries map (_.getValue), Seq("test1-value", "test2-value", "test3-value", "test4-value"))
  }
}
