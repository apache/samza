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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.samza.Partition
import org.apache.samza.metrics.{Gauge, MetricsRegistryMap}
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.{StreamMetadataCache, SystemAdmin, SystemStream, SystemStreamMetadata}
import org.junit.Assert._
import org.junit.Test
import org.mockito.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConversions


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
      .thenReturn(finalMetadata)  // Called from monitor thread the second time

    val metrics = new MetricsRegistryMap()

    val partitionCountMonitor = new StreamPartitionCountMonitor(
      JavaConversions.setAsJavaSet(inputSystemStreamSet),
      mockMetadataCache,
      metrics,
      5
    )

    partitionCountMonitor.updatePartitionCountMetric()

    assertNotNull(partitionCountMonitor.getGauges().get(inputSystemStream))
    assertEquals(1, partitionCountMonitor.getGauges().get(inputSystemStream).getValue)

    assertNotNull(metrics.getGroup("job-coordinator"))

    val metricGroup = metrics.getGroup("job-coordinator")
    assertTrue(metricGroup.get("test-system-test-stream-partitionCount").isInstanceOf[Gauge[Int]])
    assertEquals(1, metricGroup.get("test-system-test-stream-partitionCount").asInstanceOf[Gauge[Int]].getValue)
  }

  @Test
  def testStartStopBehavior(): Unit = {
    val mockMetadataCache = new MockStreamMetadataCache
    val inputSystemStream = new SystemStream("test-system", "test-stream")
    val inputSystemStreamSet = Set[SystemStream](inputSystemStream)
    val monitor = new StreamPartitionCountMonitor(
      JavaConversions.setAsJavaSet(inputSystemStreamSet),
      mockMetadataCache,
      new MetricsRegistryMap(),
      50
    )

    assertFalse(monitor.isRunning())

    // Normal start
    monitor.start()
    assertTrue(monitor.isRunning())

    // Start should be idempotent
    monitor.start()
    assertTrue(monitor.isRunning())

    // Normal stop
    monitor.stop()
    assertTrue(monitor.awaitTermination(5, TimeUnit.SECONDS));
    assertFalse(monitor.isRunning())

    // Cannot restart a stopped instance
    try
    {
      monitor.start()
      fail("IllegalStateException should have been thrown")
    } catch {
      case e: IllegalStateException => assertTrue(true)
      case _: Throwable => fail("IllegalStateException should have been thrown")
    }
    assertFalse(monitor.isRunning())

    // Stop should be idempotent
    monitor.stop()
    assertFalse(monitor.isRunning())
  }

  @Test
  def testScheduler(): Unit = {
    val mockMetadataCache = new MockStreamMetadataCache
    val inputSystemStream = new SystemStream("test-system", "test-stream")
    val inputSystemStreamSet = Set[SystemStream](inputSystemStream)
    val sampleCount = new CountDownLatch(2); // Verify 2 invocations

    val monitor = new StreamPartitionCountMonitor(
      JavaConversions.setAsJavaSet(inputSystemStreamSet),
      mockMetadataCache,
      new MetricsRegistryMap(),
      50
    ) {
      override def updatePartitionCountMetric(): Unit = {
        sampleCount.countDown()
      }
    }

    monitor.start()
    try {
      if (!sampleCount.await(5, TimeUnit.SECONDS)) {
        fail("Did not see all metric updates. Remaining count: " + sampleCount.getCount)
      }
    } finally {
      monitor.stop()
    }
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
