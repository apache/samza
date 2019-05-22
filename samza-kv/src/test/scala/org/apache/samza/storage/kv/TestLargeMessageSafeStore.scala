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

import java.util._

import org.apache.samza.config.Config
import org.apache.samza.metrics.Counter
import org.apache.samza.serializers.{JsonSerdeV2, LongSerde, StringSerde}
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.task.MessageCollector
import org.apache.samza.{Partition, SamzaException}
import org.junit.{Before, Test}
import org.mockito.Matchers
import org.mockito.Mockito._
import org.junit.Assert._

class TestLargeMessageSafeStore {

  val store = mock(classOf[KeyValueStore[Array[Byte], Array[Byte]]])
  val storeName = "testStore"
  val storeConfig = mock(classOf[Config])
  val systemStreamPartition = mock(classOf[SystemStreamPartition])
  val collector = mock(classOf[MessageCollector])
  val metrics = mock(classOf[LoggedStoreMetrics])
  val maxMessageSize = 1024
  val counter = mock(classOf[Counter])

  @Before
  def setup: Unit = {
    when(systemStreamPartition.getPartition).thenReturn(new Partition(1))
    when(storeConfig.getInt(Matchers.eq("changelog.max.message.size.bytes"), Matchers.any())).thenReturn(maxMessageSize)
    when(metrics.puts).thenReturn(counter)
    when(counter.inc()).thenReturn(1)
    doNothing().when(collector).send(Matchers.any())
    doNothing().when(store).put(Matchers.any(), Matchers.any())
    doNothing().when(store).putAll(Matchers.any())
  }

  @Test
  def testLargeMessagePutWithDropLargeMessageDisabled = {
    val largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, false, maxMessageSize)

    val key1 = new Array[Byte](16)
    val smallMessage = new Array[Byte](32)
    largeMessageSafeKeyValueStore.put(key1, smallMessage)
    verify(store, times(1)).put(Matchers.eq(key1), Matchers.eq(smallMessage))

    val key2 = new Array[Byte](16)
    val largeMessage = new Array[Byte](maxMessageSize + 1)
    try {
      largeMessageSafeKeyValueStore.put(key2, largeMessage)
      fail("The test case should have failed due to a large message being passed to the changelog, but it didn't.")
    } catch {
      case e: SamzaException =>
        verify(store, times(0)).put(Matchers.eq(key2), Matchers.eq(largeMessage))
    }
  }

  @Test
  def testSmallMessagePutAllSuccessWithDropLargeMessageDisabled = {
    val largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, false, maxMessageSize)
    val key = new Array[Byte](16)
    val smallMessage = new Array[Byte](32)

    val entries = new ArrayList[Entry[Array[Byte], Array[Byte]]]
    entries.add(new Entry(key, smallMessage))

    largeMessageSafeKeyValueStore.putAll(entries)
    verify(store, times(1)).putAll(Matchers.eq(entries))
    verify(store, times(0)).put(Matchers.any(classOf[Array[Byte]]), Matchers.any(classOf[Array[Byte]]))
  }

  @Test
  def testLargeMessagePutAllFailureWithDropLargeMessageDisabled = {
    val largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, false, maxMessageSize)
    val key = new Array[Byte](16)
    val largeMessage = new Array[Byte](maxMessageSize + 1)

    val entries = new ArrayList[Entry[Array[Byte], Array[Byte]]]
    entries.add(new Entry(key, largeMessage))

    try {
      largeMessageSafeKeyValueStore.putAll(entries)
      fail("The test case should have failed due to a large message being passed to the changelog, but it didn't.")
    } catch {
      case e: SamzaException =>
        verify(store, times(0)).putAll(Matchers.eq(entries))
        verify(store, times(0)).put(Matchers.any(classOf[Array[Byte]]), Matchers.any(classOf[Array[Byte]]))
    }
  }

  @Test
  def testSmallMessagePutWithSerdeAndDropLargeMessageDisabled(): Unit = {
    val largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, false, maxMessageSize)

    val longSerde = new LongSerde
    val longObj = 1000L
    val key = longSerde.toBytes(longObj)

    val jsonSerde = new JsonSerdeV2[java.util.HashMap[String, Object]]
    val obj = new java.util.HashMap[String, Object]();
    obj.put("jack", "jill")
    obj.put("john", new java.lang.Integer(2))
    val smallMessage = jsonSerde.toBytes(obj)

    val entries = new ArrayList[Entry[Array[Byte], Array[Byte]]]
    entries.add(new Entry(key, smallMessage))

    largeMessageSafeKeyValueStore.putAll(entries)
    verify(store, times(1)).putAll(Matchers.eq(entries))
    verify(store, times(0)).put(Matchers.any(classOf[Array[Byte]]), Matchers.any(classOf[Array[Byte]]))
  }

  @Test
  def testLargeMessagePutWithSerdeAndDropLargeMessageDisabled(): Unit = {
    val largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, false, maxMessageSize)

    val longSerde = new LongSerde
    val longObj = 1000L
    val key = longSerde.toBytes(longObj)

    val stringSerde = new StringSerde
    val largeString = "a" * maxMessageSize + 1
    val largeMessage = stringSerde.toBytes(largeString)

    val entries = new ArrayList[Entry[Array[Byte], Array[Byte]]]
    entries.add(new Entry(key, largeMessage))

    try{
      largeMessageSafeKeyValueStore.putAll(entries)
    }
    catch {
      case e: SamzaException =>
        verify(store, times(0)).putAll(Matchers.eq(entries))
        verify(store, times(0)).put(Matchers.any(classOf[Array[Byte]]), Matchers.any(classOf[Array[Byte]]))
    }
  }

  @Test
  def testPutSuccessWithDropLargeMessageEnabled = {
    val largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, true, maxMessageSize)
    val key1 = new Array[Byte](16)
    val largeMessage = new Array[Byte](maxMessageSize + 1)

    largeMessageSafeKeyValueStore.put(key1, largeMessage)

    verify(store, times(0)).put(Matchers.any(classOf[Array[Byte]]), Matchers.any(classOf[Array[Byte]]))
  }

  @Test
  def testPutAllSuccessWithDropLargeMessageEnabled = {
    val largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, true, maxMessageSize)
    val key1 = new Array[Byte](16)
    val largeMessage = new Array[Byte](maxMessageSize + 1)
    val key2 = new Array[Byte](8)
    val smallMessage = new Array[Byte](1)

    val entries = new ArrayList[Entry[Array[Byte], Array[Byte]]]
    val largeMessageEntry = new Entry(key1, largeMessage)
    val smallMessageEntry = new Entry(key2, smallMessage)
    entries.add(largeMessageEntry)
    entries.add(smallMessageEntry)

    largeMessageSafeKeyValueStore.putAll(entries)

    entries.remove(largeMessageEntry)
    verify(store, times(1)).putAll(Matchers.eq(entries))
    verify(store, times(0)).put(Matchers.any(classOf[Array[Byte]]), Matchers.any(classOf[Array[Byte]]))
  }
}
