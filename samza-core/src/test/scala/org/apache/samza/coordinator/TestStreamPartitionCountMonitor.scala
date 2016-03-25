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

package org.apache.samza.coordinator

import org.apache.samza.Partition
import org.apache.samza.metrics.{Gauge, MetricsRegistryMap}
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.{SystemAdmin, StreamMetadataCache, SystemStream, SystemStreamMetadata}
import org.junit.Assert._
import org.junit.Test
import org.mockito.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mock.MockitoSugar

class TestStreamPartitionCountMonitor extends AssertionsForJUnit with MockitoSugar {

  @Test
  def testStreamPartitionCountMonitor(): Unit = {
    val mockMetadataCache = mock[StreamMetadataCache]
    val inputSystemStream = new SystemStream("test-system", "test-stream")
    val inputSystemStreamSet = Set[SystemStream](inputSystemStream)

    val initialPartitionMetadata = new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(new Partition(0), new SystemStreamPartitionMetadata("", "", ""))
        put(new Partition(1), new SystemStreamPartitionMetadata("", "", ""))
      }
    }

    val finalPartitionMetadata = new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        putAll(initialPartitionMetadata)
        put(new Partition(2), new SystemStreamPartitionMetadata("", "", ""))
      }
    }
    val initialMetadata = Map(
      inputSystemStream -> new SystemStreamMetadata(inputSystemStream.getStream, initialPartitionMetadata)
    )
    val finalMetadata = Map(
      inputSystemStream -> new SystemStreamMetadata(inputSystemStream.getStream, finalPartitionMetadata)
    )

    when(mockMetadataCache.getStreamMetadata(any(classOf[Set[SystemStream]]), Matchers.eq(true)))
      .thenReturn(initialMetadata)  // Called during StreamPartitionCountMonitor instantiation
      .thenReturn(initialMetadata)  // Called when monitor thread is started
      .thenReturn(finalMetadata)  // Called from monitor thread the second time
      .thenReturn(finalMetadata)

    val partitionCountMonitor = new StreamPartitionCountMonitor(
      inputSystemStreamSet,
      mockMetadataCache,
      new MetricsRegistryMap(),
      5
    )

    partitionCountMonitor.startMonitor()
    Thread.sleep(50)
    partitionCountMonitor.stopMonitor()

    assertNotNull(partitionCountMonitor.gauges.get(inputSystemStream))
    assertEquals(1, partitionCountMonitor.gauges.get(inputSystemStream).getValue)

    assertNotNull(partitionCountMonitor.metrics.getGroup("job-coordinator"))

    val metricGroup = partitionCountMonitor.metrics.getGroup("job-coordinator")
    assertTrue(metricGroup.get("test-system-test-stream-partitionCount").isInstanceOf[Gauge[Int]])
    assertEquals(1, metricGroup.get("test-system-test-stream-partitionCount").asInstanceOf[Gauge[Int]].getValue)
  }

  @Test
  def testStartStopBehavior(): Unit = {
    val mockMetadataCache = new MockStreamMetadataCache
    val inputSystemStream = new SystemStream("test-system", "test-stream")
    val inputSystemStreamSet = Set[SystemStream](inputSystemStream)
    val monitor = new StreamPartitionCountMonitor(
      inputSystemStreamSet,
      mockMetadataCache,
      new MetricsRegistryMap(),
      50
    )
    monitor.stopMonitor()
    monitor.startMonitor()
    assertTrue(monitor.isRunning())
    monitor.startMonitor()
    assertTrue(monitor.isRunning())
    monitor.stopMonitor()
    assertFalse(monitor.isRunning())
    monitor.startMonitor()
    assertTrue(monitor.isRunning())
    monitor.stopMonitor()
    assertFalse(monitor.isRunning())
    monitor.stopMonitor()
    assertFalse(monitor.isRunning())
  }

  class MockStreamMetadataCache extends StreamMetadataCache(Map[String, SystemAdmin]()) {
    /**
     * Returns metadata about each of the given streams (such as first offset, newest
     * offset, etc). If the metadata isn't in the cache, it is retrieved from the systems
     * using the given SystemAdmins.
     */

    override def getStreamMetadata(streams: Set[SystemStream], partitionsMetadataOnly: Boolean): Map[SystemStream, SystemStreamMetadata] = {
      val inputSystemStream = new SystemStream("test-system", "test-stream")
      val initialPartitionMetadata = new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
        {
          put(new Partition(0), new SystemStreamPartitionMetadata("", "", ""))
          put(new Partition(1), new SystemStreamPartitionMetadata("", "", ""))
        }
      }

      val initialMetadata = Map(
        inputSystemStream -> new SystemStreamMetadata(inputSystemStream.getStream, initialPartitionMetadata)
      )
      initialMetadata
    }
  }
}
