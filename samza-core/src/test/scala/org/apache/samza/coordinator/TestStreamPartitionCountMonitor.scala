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
import java.util.HashMap
import org.apache.samza.Partition
import org.apache.samza.metrics.{Gauge, MetricsRegistryMap}
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system._
import org.junit.Assert._
import org.junit.Test
import org.mockito.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._


class TestStreamPartitionCountMonitor extends AssertionsForJUnit with MockitoSugar {

  @Test
  def testStreamPartitionCountChange(): Unit = {
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

    val mockCallback = mock[StreamPartitionCountMonitor.Callback]

    val partitionCountMonitor = new StreamPartitionCountMonitor(
      inputSystemStreamSet.asJava,
      mockMetadataCache,
      metrics,
      5,
      mockCallback
    )

    partitionCountMonitor.updatePartitionCountMetric()

    assertNotNull(partitionCountMonitor.getGauges().get(inputSystemStream))
    assertEquals(1, partitionCountMonitor.getGauges().get(inputSystemStream).getValue)

    assertNotNull(metrics.getGroup("job-coordinator"))

    val metricGroup = metrics.getGroup("job-coordinator")
    assertTrue(metricGroup.get("test-system-test-stream-partitionCount").isInstanceOf[Gauge[Int]])
    assertEquals(1, metricGroup.get("test-system-test-stream-partitionCount").asInstanceOf[Gauge[Int]].getValue)

    verify(mockCallback, times(1)).onSystemStreamPartitionChange(any())

  }

  @Test
  def testStreamPartitionCountException(): Unit = {
    val mockMetadataCache = mock[StreamMetadataCache]
    val inputSystemStream = new SystemStream("test-system", "test-stream")
    val inputExceptionStream = new SystemStream("test-system", "test-exception-stream")
    val inputSystemStreamSet = Set[SystemStream](inputSystemStream, inputExceptionStream)

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
    val streamMockMetadata = mock[java.util.HashMap[Partition, SystemStreamPartitionMetadata]]

    val initialMetadata = Map(
      inputExceptionStream -> new SystemStreamMetadata(inputExceptionStream.getStream, initialPartitionMetadata),
      inputSystemStream -> new SystemStreamMetadata(inputSystemStream.getStream, initialPartitionMetadata)
    )
    val finalMetadata = Map(
      inputExceptionStream -> new SystemStreamMetadata(inputExceptionStream.getStream, streamMockMetadata),
      inputSystemStream -> new SystemStreamMetadata(inputSystemStream.getStream, finalPartitionMetadata)
    )

    when(mockMetadataCache.getStreamMetadata(any(classOf[Set[SystemStream]]), Matchers.eq(true)))
      .thenReturn(initialMetadata)  // Called during StreamPartitionCountMonitor instantiation
      .thenReturn(finalMetadata)  // Called from monitor thread the second time

    // make the call to get stream metadata for {@code inputExceptionStream} fail w/ a runtime exception
    when(streamMockMetadata.keySet()).thenThrow(new RuntimeException)

    val metrics = new MetricsRegistryMap()

    val mockCallback = mock[StreamPartitionCountMonitor.Callback]

    val partitionCountMonitor = new StreamPartitionCountMonitor(
      inputSystemStreamSet.asJava,
      mockMetadataCache,
      metrics,
      5,
      mockCallback
    )

    partitionCountMonitor.updatePartitionCountMetric()

    assertNotNull(partitionCountMonitor.getGauges().get(inputSystemStream))
    assertEquals(1, partitionCountMonitor.getGauges().get(inputSystemStream).getValue)

    assertNotNull(metrics.getGroup("job-coordinator"))

    val metricGroup = metrics.getGroup("job-coordinator")
    assertTrue(metricGroup.get("test-system-test-stream-partitionCount").isInstanceOf[Gauge[Int]])
    assertEquals(1, metricGroup.get("test-system-test-stream-partitionCount").asInstanceOf[Gauge[Int]].getValue)

    // Make sure as long as one of the input stream topic partition change is detected, the callback is invoked
    verify(mockCallback, times(1)).onSystemStreamPartitionChange(any())

  }

  @Test
  def testStartStopBehavior(): Unit = {
    val mockMetadataCache = new MockStreamMetadataCache
    val inputSystemStream = new SystemStream("test-system", "test-stream")
    val inputSystemStreamSet = Set[SystemStream](inputSystemStream)
    val monitor = new StreamPartitionCountMonitor(
      inputSystemStreamSet.asJava,
      mockMetadataCache,
      new MetricsRegistryMap(),
      50,
      null
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
      inputSystemStreamSet.asJava,
      mockMetadataCache,
      new MetricsRegistryMap(),
      50,
      null
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

  class MockStreamMetadataCache extends StreamMetadataCache(new SystemAdmins(new HashMap[String, SystemAdmin])) {
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
