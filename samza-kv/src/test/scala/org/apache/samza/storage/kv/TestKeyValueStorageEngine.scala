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

import java.util.Arrays

import org.apache.samza.Partition
import org.apache.samza.storage.StoreProperties
import org.apache.samza.system.{IncomingMessageEnvelope, SystemStreamPartition}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.mockito.Mockito._

class TestKeyValueStorageEngine {
  var engine: KeyValueStorageEngine[String, String] = null
  var metrics: KeyValueStorageEngineMetrics = null;

  @Before
  def setup() {
    val wrapperKv = new MockKeyValueStore()
    val rawKv = mock(classOf[KeyValueStore[Array[Byte], Array[Byte]]])
    val properties = mock(classOf[StoreProperties])
    metrics = new KeyValueStorageEngineMetrics
    engine = new KeyValueStorageEngine[String, String](properties, wrapperKv, rawKv, metrics)
  }

  @After
  def teardown() {
    engine.close()
  }

  @Test
  def testGetAndPut(): Unit = {
    var prevGets = metrics.gets.getCount
    var prevGetNsSnapshotSize = metrics.getNs.getSnapshot.getSize
    var valueForK1 = engine.get("k1");
    assertNull("k1 is not existing before put", valueForK1);
    assertEquals("get counter increments by 1", 1, metrics.gets.getCount - prevGets)
    assertEquals("get timer has 1 additional data point" , 1,  metrics.getNs.getSnapshot.getSize - prevGetNsSnapshotSize)

    var prevPuts = metrics.puts.getCount
    var prevPutNsSnapshotSize = metrics.putNs.getSnapshot.getSize
    engine.put("k1", "v1")
    assertEquals("put counter increments by 1", 1, metrics.puts.getCount - prevPuts)
    assertEquals("put timer has 1 additional data point", 1, metrics.putNs.getSnapshot.getSize - prevPutNsSnapshotSize)

    assertEquals("k1 is existing after put and the value for k1 is v1", "v1", engine.get("k1"))
  }

  @Test
  def testDelete(): Unit = {
    engine.put("k1", "v1")
    engine.put("k2", "v2")
    assertNotNull("k1 is existing before being deleted", engine.get("k1"))
    var prevDeletes = metrics.deletes.getCount
    var prevDeleteNsSnapshotSize = metrics.deleteNs.getSnapshot.getSize
    engine.delete("k1")
    assertNull("k1 is not existing since it has been deleted", engine.get("k1"))
    assertEquals("k2 is still existing after deleting k1", "v2", engine.get("k2"))
    assertEquals("delete counter increments by 1", 1, metrics.deletes.getCount - prevDeletes)
    assertEquals("delete timer has 1 additional data point", 1, metrics.deleteNs.getSnapshot.getSize - prevDeleteNsSnapshotSize)
  }

  @Test
  def testFlush(): Unit = {
    var prevFlushes = metrics.flushes.getCount
    var prevFlushNsSnapshotSize = metrics.flushNs.getSnapshot.getSize
    engine.flush()
    assertEquals("flush counter increments by 1", 1, metrics.flushes.getCount - prevFlushes)
    assertEquals("flush timer has 1 additional data point", 1, metrics.flushNs.getSnapshot.getSize - prevFlushNsSnapshotSize)
  }


  @Test
  def testIterator() {
    val keys = Arrays.asList("k1",
      "k2",
      "k3")
    val values = Arrays.asList("v1",
      "v2",
      "v3")

    for (i <- 0 until 3) {
      engine.put(keys.get(i), values.get(i))
    }

    // test all iterator
    var prevAlls = metrics.alls.getCount
    var prevAllNsSnapshotSize = metrics.allNs.getSnapshot.getSize
    var iter = engine.all()
    assertEquals("all counter increments by 1", 1, metrics.alls.getCount - prevAlls)
    assertEquals("all timer has 1 additional data point", 1, metrics.allNs.getSnapshot.getSize - prevAllNsSnapshotSize)
    for (i <- 0 until 3) {
      assertTrue("iterator has next for 3 times", iter.hasNext)
      val entry = iter.next()
      assertEquals(entry.getKey, keys.get(i))
      assertEquals(entry.getValue, values.get(i))
    }
    assertFalse("no next after iterating all 3 keys", iter.hasNext)

    // test range iterator
    var prevRanges = metrics.ranges.getCount
    var prevRangeNsSnapshotSize = metrics.rangeNs.getSnapshot.getSize
    iter = engine.range(keys.get(0), keys.get(2))
    assertEquals("range counter increments by 1", 1, metrics.ranges.getCount - prevRanges)
    assertEquals("range timer has 1 additional data point", 1, metrics.rangeNs.getSnapshot.getSize - prevRangeNsSnapshotSize)
    for (i <- 0 until 2) { //only iterate the first 2 keys in the range
      assertTrue("iterator has next for twice", iter.hasNext)
      val entry = iter.next()
      assertEquals(entry.getKey, keys.get(i))
      assertEquals(entry.getValue, values.get(i))
    }
    assertFalse("no next after iterating 2 keys in the range", iter.hasNext)
  }

  @Test
  def testRestoreMetrics(): Unit = {
    val changelogSSP = new SystemStreamPartition("TestSystem", "TestStream", new Partition(0))
    val changelogEntries = java.util.Arrays asList(
      new IncomingMessageEnvelope(changelogSSP, "0", Array[Byte](1, 2), Array[Byte](3, 4, 5)),
      new IncomingMessageEnvelope(changelogSSP, "1", Array[Byte](2, 3), Array[Byte](4, 5, 6)),
      new IncomingMessageEnvelope(changelogSSP, "2", Array[Byte](3, 4), Array[Byte](5, 6, 7)))

    engine.restore(changelogEntries.iterator())

    assertEquals(3, metrics.restoredMessages.getValue)
    assertEquals(15, metrics.restoredBytes.getValue) // 3 keys * 2 bytes/key +  3 msgs * 3 bytes/msg
  }
}
