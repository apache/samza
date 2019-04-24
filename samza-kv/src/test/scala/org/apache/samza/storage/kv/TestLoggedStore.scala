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
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.task.MessageCollector
import org.apache.samza.{Partition, SamzaException}
import org.junit.{Before, Test}
import org.mockito.Matchers
import org.mockito.Mockito._


class TestLoggedStore {

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
  def testLargeMessagePut = {
    val safeLoggedStore = new LoggedStore(store, storeName, storeConfig, systemStreamPartition, collector, metrics)

    val key1 = new Array[Byte](16)
    val smallMessage = new Array[Byte](32)
    safeLoggedStore.put(key1, smallMessage)
    verify(store, times(1)).put(Matchers.eq(key1), Matchers.eq(smallMessage))

    val key2 = new Array[Byte](16)
    val largeMessage = new Array[Byte](1025)
    try {
      safeLoggedStore.put(key2, largeMessage)
    } catch {
      case e: SamzaException =>
        verify(store, times(0)).put(Matchers.eq(key2), Matchers.eq(largeMessage))
    }
  }

  @Test
  def testSmallMessagePutAllSuccess = {
    val safeLoggedStore = new LoggedStore(store, storeName, storeConfig, systemStreamPartition, collector, metrics)
    val key = new Array[Byte](16)
    val smallMessage = new Array[Byte](32)

    val entries = new ArrayList[Entry[Array[Byte], Array[Byte]]]
    entries.add(new Entry(key, smallMessage))

    safeLoggedStore.putAll(entries)
    verify(store, times(1)).putAll(Matchers.eq(entries))
    verify(store, times(0)).put(Matchers.any(classOf[Array[Byte]]), Matchers.any(classOf[Array[Byte]]))
  }

  @Test
  def testLargeMessagePutAllFailure = {
    val safeLoggedStore = new LoggedStore(store, storeName, storeConfig, systemStreamPartition, collector, metrics)
    val key = new Array[Byte](16)
    val largeMessage = new Array[Byte](1025)

    val entries = new ArrayList[Entry[Array[Byte], Array[Byte]]]
    entries.add(new Entry(key, largeMessage))

    try {
      safeLoggedStore.putAll(entries)
    } catch {
      case e: SamzaException =>
        verify(store, times(0)).putAll(Matchers.eq(entries))
        verify(store, times(0)).put(Matchers.any(classOf[Array[Byte]]), Matchers.any(classOf[Array[Byte]]))
    }
  }
}
