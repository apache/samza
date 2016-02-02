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

import org.apache.samza.config.{MapConfig, StorageConfig}
import org.apache.samza.serializers.Serde
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStore
import org.junit.After
import org.junit.Assert._
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.scalatest.Assertions.intercept

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Test suite to check different key value store operations
 * @param typeOfStore Defines type of key-value store (Eg: "rocksdb" / "inmemory")
 * @param storeConfig Defines whether we're using caching / serde / both / or none in front of the store
 */
@RunWith(value = classOf[Parameterized])
class TestKeyValueStores(typeOfStore: String, storeConfig: String) {
  import TestKeyValueStores._

  val letters = "abcdefghijklmnopqrstuvwxyz".map(_.toString)
  val dir = new File(System.getProperty("java.io.tmpdir"), "rocksdb-test-" + new Random().nextInt(Int.MaxValue))
  var store: KeyValueStore[Array[Byte], Array[Byte]] = null
  var cache = false
  var serde = false

  @Before
  def setup() {
    val kvStore : KeyValueStore[Array[Byte], Array[Byte]] = typeOfStore match {
      case "inmemory" =>
        new InMemoryKeyValueStore
      case "rocksdb" =>
        new RocksDbKeyValueStore (dir,
                                  new org.rocksdb.Options()
                                  .setCreateIfMissing(true)
                                  .setCompressionType(org.rocksdb.CompressionType.SNAPPY_COMPRESSION),
                                  new MapConfig(),
                                  false,
                                  "someStore")
      case _ =>
        throw new IllegalArgumentException("Type of store undefined: " + typeOfStore)
    }

    val passThroughSerde = new Serde[Array[Byte]] {
      def toBytes(obj: Array[Byte]) = obj
      def fromBytes(bytes: Array[Byte]) = bytes
    }

    store = storeConfig match {
      case "cache" =>
        cache = true
        new CachedStore(kvStore, CacheSize, BatchSize)
      case "serde" =>
        serde = true
        new SerializedKeyValueStore(kvStore, passThroughSerde, passThroughSerde)
      case "cache-and-serde" =>
        val serializedStore = new SerializedKeyValueStore(kvStore, passThroughSerde, passThroughSerde)
        serde = true
        cache = true
        new CachedStore(serializedStore, CacheSize, BatchSize)
      case _ =>
        kvStore
    }
    store = new NullSafeKeyValueStore(store)
  }

  @After
  def teardown() {
    store.close
    if (dir != null && dir.listFiles() != null) {
      for (file <- dir.listFiles)
        file.delete()
      dir.delete()
    }
  }

  @Test
  def getNonExistentIsNull() {
    assertNull(store.get(b("hello")))
  }

  @Test
  def testGetAllWhenZeroMatch() {
    store.put(b("hello"), b("world"))
    val keys = List(b("foo"), b("bar"))
    val actual = store.getAll(keys)
    keys.foreach(k => assertNull("Key: " + k, actual.get(k)))
  }

  @Test
  def testGetAllWhenFullMatch() {
    val expected = Map(b("k0") -> b("v0"), b("k1") -> b("v1"))
    expected.foreach(e => store.put(e._1, e._2))
    val actual = store.getAll(expected.keys.toList)
    assertEquals("Size", expected.size, actual.size)
    expected.foreach(e => assertArrayEquals("Value at: " + s(e._1), e._2, actual.get(e._1)))
  }

  @Test
  def testGetAllWhenPartialMatch() {
    val all = Map(b("k0") -> b("v0"), b("k1") -> b("v1"), b("k2") -> b("v2"))
    val found = all.entrySet.head
    val notFound = all.entrySet.last
    store.put(found.getKey, found.getValue)
    val actual = store.getAll(List(notFound.getKey, found.getKey))
    assertNull(actual.get(notFound.getKey))
    assertArrayEquals(found.getValue, actual.get(found.getKey))
  }

  @Test
  def putAndGet() {
    store.put(b("k"), b("v"))
    assertArrayEquals(b("v"), store.get(b("k")))
  }

  @Test
  def doublePutAndGet() {
    val k = b("k2")
    store.put(k, b("v1"))
    store.put(k, b("v3"))
    assertArrayEquals(b("v3"), store.get(k))
  }

  @Test
  def testNullsWithSerde() {
    if (serde) {
      val a = b("a")

      intercept[NullPointerException] { store.get(null) }
      intercept[NullPointerException] { store.getAll(null) }
      intercept[NullPointerException] { store.getAll(List(a, null)) }
      intercept[NullPointerException] { store.delete(null) }
      intercept[NullPointerException] { store.deleteAll(null) }
      intercept[NullPointerException] { store.deleteAll(List(a, null)) }
      intercept[NullPointerException] { store.put(null, a) }
      intercept[NullPointerException] { store.put(a, null) }
      intercept[NullPointerException] { store.putAll(List(new Entry(a, a), new Entry[Array[Byte], Array[Byte]](a, null))) }
      intercept[NullPointerException] { store.putAll(List(new Entry[Array[Byte], Array[Byte]](null, a))) }
      intercept[NullPointerException] { store.range(a, null) }
      intercept[NullPointerException] { store.range(null, a) }
    }
  }

  @Test
  def testPutAll() {
    // Use CacheSize - 1 so we fully fill the cache, but don't write any data 
    // out. Our check (below) uses == for cached entries, and using 
    // numEntires >= CacheSize would result in the LRU cache dropping some 
    // entries. The result would be that we get the correct byte array back 
    // from the cache's underlying store (rocksdb), but that == would fail.
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
    checkRange(letters.slice(from, to), iter)
    iter.close()
  }

  @Test
  def testDelete() {
    val a = b("a")
    assertNull(store.get(a))
    store.put(a, a)
    assertArrayEquals(a, store.get(a))
    store.delete(a)
    assertNull(store.get(a))
  }

  @Test
  def testDeleteAllWhenZeroMatch() {
    val foo = b("foo")
    store.put(foo, foo)
    store.deleteAll(List(b("bar")))
    assertArrayEquals(foo, store.get(foo))
  }

  @Test
  def testDeleteAllWhenFullMatch() {
    val all = Map(b("k0") -> b("v0"), b("k1") -> b("v1"))
    all.foreach(e => store.put(e._1, e._2))
    assertEquals(all.size, store.getAll(all.keys.toList).size)
    store.deleteAll(all.keys.toList)
    all.keys.foreach(key => assertNull("Value at: " + s(key), store.get(key)))
  }

  @Test
  def testDeleteAllWhenPartialMatch() {
    val all = Map(b("k0") -> b("v0"), b("k1") -> b("v1"))
    val found = all.entrySet.head
    val leftAlone = all.entrySet.last
    all.foreach(e => store.put(e._1, e._2))
    assertArrayEquals(found.getValue, store.get(found.getKey))
    store.deleteAll(List(b("not found"), found.getKey))
    store.flush()
    val allIterator = store.all
    try {
      assertEquals(1, allIterator.size)
      assertArrayEquals(leftAlone.getValue, store.get(leftAlone.getKey))
    } finally {
      allIterator.close()
    }
  }

  @Test
  def testSimpleScenario() {
    val vals = letters.map(b(_))
    for (v <- vals) {
      assertNull(store.get(v))
      store.put(v, v)
      assertArrayEquals(v, store.get(v))
    }
    vals.foreach(v => assertArrayEquals(v, store.get(v)))
    vals.foreach(v => store.delete(v))
    vals.foreach(v => assertNull(store.get(v)))
  }

  /**
   * This test specifically targets an issue in Scala 2.8.1's DoubleLinkedList
   * implementation. The issue is that it doesn't work. More specifically,
   * creating a DoubleLinkedList from an existing list does not update the
   * "prev" field of the existing list's head to point to the new head. As a
   * result, in Scala 2.8.1, every DoubleLinkedList node's prev field is null.
   * Samza gets around this by manually updating the field itself. See SAMZA-80
   * for details.
   *
   * This issue is exposed in Samza's KV cache implementation, which uses
   * DoubleLinkedList, so all comments in this method are discussing the cached
   * implementation, but the test is still useful as a sanity check for
   * non-cached stores.
   */
  @Test
  def testBrokenScalaDoubleLinkedList() {
    val something = b("")
    val keys = letters
            .map(b(_))
            .toArray

    // Load the cache to capacity.
    letters
            .slice(0, TestKeyValueStores.CacheSize)
            .map(b(_))
            .foreach(store.put(_, something))

    // Now keep everything in the cache, but with an empty dirty list.
    store.flush

    // Dirty list is now empty, and every CacheEntry has dirty=null.

    // Corrupt the dirty list by creating two dirty lists that toggle back and 
    // forth depending on whether the last dirty write was to 1 or 0. The trick
    // here is that every element in the cache is treated as the "head" of the
    // DoulbeLinkedList (prev==null), even though it's not necessarily. Thus,
    // You can end up with multiple nodes each having their own version of the 
    // dirty list with different elements in them.
    store.put(keys(1), something)
    store.put(keys(0), something)
    store.put(keys(1), something)
    store.flush
    // The dirty list is now empty, but 0's dirty field actually has 0 and 1.
    store.put(keys(0), something)
    // The dirty list now has 0 and 1, but 1's dirty field is null in the 
    // cache because it was just flushed.

    // Get rid of 1 from the cache by reading every other element, and then 
    // putting one new element.
    letters
            .slice(2, TestKeyValueStores.CacheSize)
            .map(b(_))
            .foreach(store.get(_))
    store.put(keys(10), something)

    // Now try and trigger an NPE since the dirty list has an element (1) 
    // that's no longer in the cache.
    store.flush
  }

  /**
   * A little test that tries to simulate a few common patterns:
   * read-modify-write, and do-some-stuff-then-delete (windowing).
   */
  @Test
  def testRandomReadWriteRemove() {
    // Make test deterministic by seeding the random number generator.
    val rand = new Random(12345)
    val keys = letters
            .map(b(_))
            .toArray

    // Map from letter to key byte array used for letter, and expected value.
    // We have to go through some acrobatics here since Java's byte array uses 
    // object identity for .equals. Two byte arrays can have identical byte 
    // elements, but not be equal.
    var expected = Map[String, (Array[Byte], String)]()

    (0 until 100).foreach(loop => {
      (0 until 30).foreach(i => {
        val idx = rand.nextInt(keys.length)
        val randomValue = letters(rand.nextInt(keys.length))
        val key = keys(idx)
        val currentVal = store.get(key)
        store.put(key, b(randomValue))
        expected += letters(idx) -> (key, randomValue)
      })

      for ((k, v) <- expected) {
        val bytes = store.get(v._1)
        assertNotNull(bytes)
        assertEquals(v._2, new String(bytes, "UTF-8"))
      }

      // Iterating and making structural modifications (deletion) does not look right.
      // Separating these two steps
      val iterator = store.all
      val allKeys = new ArrayBuffer[Array[Byte]]()

      // First iterate
      while (iterator.hasNext) {
        allKeys += iterator.next.getKey
      }
      iterator.close

      // And now delete
      for (key <- allKeys) {
        store.delete(key)
        expected -= new String(key, "UTF-8")
      }

      assertEquals(0, expected.size)
    })
  }

  def checkRange(vals: IndexedSeq[String], iter: KeyValueIterator[Array[Byte], Array[Byte]]) {
    for (v <- vals) {
      assertTrue(iter.hasNext)
      val entry = iter.next()
      assertEquals(v, s(entry.getKey))
      assertEquals(v, s(entry.getValue))
    }
    assertFalse(iter.hasNext)
    intercept[NoSuchElementException] { iter.next() }
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
  val CacheSize = 1024
  val BatchSize = 1024
  @Parameters
  def parameters: java.util.Collection[Array[String]] = Arrays.asList(
      //Inmemory
      Array("inmemory", "cache"),
      Array("inmemory", "serde"),
      Array("inmemory", "cache-and-serde"),
      Array("inmemory", "none"),
      //RocksDB
      Array("rocksdb","cache"),
      Array("rocksdb","serde"),
      Array("rocksdb","cache-and-serde"),
      Array("rocksdb","none"))
}
