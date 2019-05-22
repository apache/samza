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
import java.util.{ArrayList, Arrays, Random}

import org.apache.samza.config.MapConfig
import org.apache.samza.serializers.StringSerde
import org.apache.samza.storage.kv.TestLargeMessageSafeKeyValueStores.{BatchSize, CacheSize}
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStore
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemStreamPartition}
import org.apache.samza.task.MessageCollector
import org.apache.samza.{Partition, SamzaException}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.{After, Before, Test}
import org.scalatest.Assertions.fail

@RunWith(value = classOf[Parameterized])
class TestLargeMessageSafeKeyValueStores(typeOfStore: String, storeConfig: String, dropLargeMessageStr: String) {

  val dir = new File(System.getProperty("java.io.tmpdir"), "rocksdb-test-" + new Random().nextInt(Int.MaxValue))
  var store: KeyValueStore[String, String] = null
  val stringSerde = new StringSerde
  var loggedStore: KeyValueStore[Array[Byte], Array[Byte]] = null
  val storeName = "testStore"
  val systemStreamPartition = new SystemStreamPartition("test-system", "test-stream", new Partition(0))
  val collector = new MessageCollector {
    override def send(envelope: OutgoingMessageEnvelope): Unit = {}
  }
  val loggedStoreMetrics = new LoggedStoreMetrics(storeName)
  val dropLargeMessage = dropLargeMessageStr.toBoolean
  val maxMessageSize = 1024

  @Before
  def setup() {

    val kvStore: KeyValueStore[Array[Byte], Array[Byte]] = typeOfStore match {
      case "inmemory" =>
        new InMemoryKeyValueStore
      case "rocksdb" =>
        new RocksDbKeyValueStore(
          dir,
          new org.rocksdb.Options()
            .setCreateIfMissing(true)
            .setCompressionType(org.rocksdb.CompressionType.SNAPPY_COMPRESSION),
          new MapConfig(),
          false,
          "someStore")
      case _ =>
        throw new IllegalArgumentException("Type of store undefined: " + typeOfStore)
    }

    loggedStore = new LoggedStore(kvStore, systemStreamPartition, collector, loggedStoreMetrics)

    store = storeConfig match {
      case "serde" =>
        val largeMessageSafeStore = new LargeMessageSafeStore(loggedStore, storeName, dropLargeMessage, maxMessageSize)
        new SerializedKeyValueStore(largeMessageSafeStore, stringSerde, stringSerde)
      case "cache-then-serde" =>
        var toBeSerializedStore: KeyValueStore[Array[Byte], Array[Byte]] = loggedStore
        toBeSerializedStore = new LargeMessageSafeStore(loggedStore, storeName, dropLargeMessage, maxMessageSize)
        val serializedStore = new SerializedKeyValueStore(toBeSerializedStore, stringSerde, stringSerde)
        new CachedStore(serializedStore, CacheSize, BatchSize)
      case "serde-then-cache" =>
        val cachedStore = new CachedStore(loggedStore, CacheSize, BatchSize)
        val largeMessageSafeStore = new LargeMessageSafeStore(cachedStore, storeName, dropLargeMessage, maxMessageSize)
        new SerializedKeyValueStore(largeMessageSafeStore, stringSerde, stringSerde)
      case _ =>
        throw new IllegalArgumentException("Store config undefined: " + storeConfig)
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
  def testLargeMessagePutFailureForLoggedStoreWithWrappedStore(): Unit = {
    val key = "test"
    val largeMessage = "x" * (maxMessageSize + 1)

    if (dropLargeMessage) {
      store.put(key, largeMessage)
      assert(loggedStore.get(stringSerde.toBytes(key)) == null, "The large message was stored while it shouldn't have been.")
    } else {
      try {
        store.put(key, largeMessage)
        fail("Failure since putAll() method invocation incorrectly completed.")
      } catch {
        case e: SamzaException =>
          assert(store.get(key) == null, "The large message was stored while it shouldn't have been.")
      }
    }
  }

  @Test
  def testLargeMessagePutAllFailureForLoggedStoreWithWrappedStore(): Unit = {
    val key = "test"
    val largeMessage = "x" * (maxMessageSize + 1)
    val entries = new ArrayList[Entry[String, String]]
    entries.add(new Entry(key, largeMessage))

    if (dropLargeMessage) {
      store.putAll(entries)
      assert(loggedStore.get(stringSerde.toBytes(key)) == null, "The large message was stored while it shouldn't have been.")
    } else {
      try {
        store.putAll(entries)
        fail("Failure since putAll() method invocation incorrectly completed.")
      } catch {
        case e: SamzaException =>
          assert(store.get(key) == null, "The large message was stored while it shouldn't have been.")
      }
    }
  }

  @Test
  def testSmallMessagePutSuccessForLoggedStoreWithWrappedStore(): Unit = {
    val key = "test"
    val smallMessage = "x" * (maxMessageSize - 1)

    store.put(key, smallMessage)
    assert(store.get(key).equals(smallMessage))
  }

  @Test
  def testSmallMessagePutAllSuccessForLoggedStoreWithWrappedStore(): Unit = {
    val key = "test"
    val smallMessage = "x" * (maxMessageSize - 1)

    val entries = new ArrayList[Entry[String, String]]
    entries.add(new Entry(key, smallMessage))

    store.putAll(entries)
    assert(store.get(key).equals(smallMessage))
  }
}

object TestLargeMessageSafeKeyValueStores {
  val CacheSize = 1024
  val BatchSize = 1

  @Parameters
  def parameters: java.util.Collection[Array[String]] = Arrays.asList(
    //Inmemory
    Array("inmemory", "serde", "true"),
    Array("inmemory", "serde", "false"),
    Array("inmemory", "cache-then-serde", "true"),
    Array("inmemory", "serde-then-cache", "false"),
    Array("inmemory", "serde-then-cache", "true"),
    //RocksDB
    Array("rocksdb", "serde", "true"),
    Array("rocksdb", "serde", "false"),
    Array("rocksdb", "cache-then-serde", "true"),
    Array("rocksdb", "serde-then-cache", "false"),
    Array("rocksdb", "serde-then-cache", "true")
  )
}
