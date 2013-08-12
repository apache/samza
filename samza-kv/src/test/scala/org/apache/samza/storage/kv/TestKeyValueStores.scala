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

import java.io.File
import java.util.Arrays
import java.util.Random

import scala.collection.JavaConversions._

import org.iq80.leveldb.Options
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

@RunWith(value = classOf[Parameterized])
class TestKeyValueStores(cache: Boolean) {

  import TestKeyValueStores._

  val letters = "abcdefghijklmnopqrstuvwxyz".map(_.toString)
  val dir = new File(System.getProperty("java.io.tmpdir"), "leveldb-test-" + new Random().nextInt(Int.MaxValue))
  var store: KeyValueStore[Array[Byte], Array[Byte]] = null

  @Before
  def setup() {
    dir.mkdirs()
    val leveldb = new LevelDbKeyValueStore(dir, new Options)
    if (cache)
      store = new CachedStore(leveldb, CacheSize, BatchSize)
    else
      store = leveldb
  }

  @After
  def teardown() {
    for (file <- dir.listFiles)
      file.delete()
    dir.delete()
  }

  @Test
  def getNonExistantIsNull() {
    assertNull(store.get(b("hello")))
  }

  @Test
  def putAndGet() {
    store.put(b("k"), b("v"))
    assertTrue(Arrays.equals(b("v"), store.get(b("k"))))
  }

  @Test
  def doublePutAndGet() {
    val k = b("k2")
    store.put(k, b("v1"))
    store.put(k, b("v2"))
    assertTrue(Arrays.equals(b("v2"), store.get(k)))
  }

  @Test
  def testPutAll() {
    // Use CacheSize - 1 so we fully fill the cache, but don't write any data 
    // out. Our check (below) uses == for cached entries, and using 
    // numEntires >= CacheSize would result in the LRU cache dropping some 
    // entries. The result would be that we get the correct byte array back 
    // from the cache's underlying store (leveldb), but that == would fail.
    val numEntries = CacheSize - 1
    val entries = (0 until numEntries).map(i => new Entry(b("k" + i), b("v" + i)))
    store.putAll(entries)
    if (cache) {
      assertTrue("All values should be found and cached.", entries.forall(e => store.get(e.getKey) == e.getValue))
    } else {
      assertTrue("All values should be found.", entries.forall(e => Arrays.equals(store.get(e.getKey), e.getValue)))
    }
  }

  @Test
  def testIterateAll() {
    for (letter <- letters)
      store.put(b(letter.toString), b(letter.toString))
    val iter = store.all
    checkRange(letters, iter)
    iter.close()
  }

  @Test
  def testRange() {
    val from = 5
    val to = 20
    for (letter <- letters)
      store.put(b(letter.toString), b(letter.toString))
    val iter = store.range(b(letters(from)), b(letters(to)))
    checkRange(letters.slice(from, to + 1), iter)
    iter.close()
  }

  @Test
  def testDelete() {
    val a = b("a")
    store.put(a, a)
    store.delete(a)
    assertNull(store.get(a))
  }

  @Test
  def testSimpleScenario() {
    val vals = letters.map(b(_))
    for (v <- vals) {
      assertNull(store.get(v))
      store.put(v, v)
      assertTrue(Arrays.equals(v, store.get(v)))
    }
    vals.foreach(v => assertTrue(Arrays.equals(v, store.get(v))))
    vals.foreach(v => store.delete(v))
    vals.foreach(v => assertNull(store.get(v)))
  }

  def checkRange(vals: IndexedSeq[String], iter: KeyValueIterator[Array[Byte], Array[Byte]]) {
    for (v <- vals) {
      assertTrue(iter.hasNext)
      val entry = iter.next()
      assertEquals(v, s(entry.getKey))
      assertEquals(v, s(entry.getValue))
    }
    assertFalse(iter.hasNext)
  }

  /**
   * Convert string to byte buffer
   */
  def b(s: String) =
    s.getBytes

  /**
   * Convert byte buffer to string
   */
  def s(b: Array[Byte]) =
    new String(b)

}

object TestKeyValueStores {
  val CacheSize = 10
  val BatchSize = 2
  @Parameters
  def parameters: java.util.Collection[Array[java.lang.Boolean]] = Arrays.asList(Array(java.lang.Boolean.TRUE), Array(java.lang.Boolean.FALSE))
}
