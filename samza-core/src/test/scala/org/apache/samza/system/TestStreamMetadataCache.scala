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

package org.apache.samza.system

import java.util
import java.util.concurrent.{Callable, Executors}

import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.util.Clock
import org.apache.samza.{Partition, SamzaException}
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers => ScalaTestMatchers}

import scala.collection.JavaConverters._

class TestStreamMetadataCache extends AssertionsForJUnit with MockitoSugar with ScalaTestMatchers {
  private val SYSTEM = "system"
  private val EXTENDED_SYSTEM = "extendedSystem"

  @Mock
  var systemAdmin: SystemAdmin = _
  @Mock
  var extendedSystemAdmin: ExtendedSystemAdmin = _
  @Mock
  var clock: Clock = _
  var streamMetadataCache: StreamMetadataCache = _

  @Before
  def setup(): Unit = {
    MockitoAnnotations.initMocks(this)
    val systemAdmins = Map(SYSTEM -> systemAdmin, EXTENDED_SYSTEM -> extendedSystemAdmin)
    streamMetadataCache = new StreamMetadataCache(systemAdmins = new SystemAdmins(systemAdmins.asJava), clock = clock)
  }

  private def makeMetadata(streamNames: Set[String] = Set("stream"), numPartitions: Int = 4):
    Map[String, SystemStreamMetadata] = {
    val partitions = (0 until numPartitions).map(partition => {
      new Partition(partition) -> new SystemStreamPartitionMetadata("oldest", "newest", "upcoming")
    }).toMap
    streamNames.map(name => name -> new SystemStreamMetadata(name, partitions.asJava)).toMap
  }

  @Test
  def testFetchUncachedMetadataFromSystemAdmin() {
    when(systemAdmin.getSystemStreamMetadata(Set("bar").asJava)).thenReturn(makeMetadata(Set("bar")).asJava)
    val streams = Set(new SystemStream(SYSTEM, "bar"))

    val result = streamMetadataCache.getStreamMetadata(streams)
    streams shouldEqual result.keySet
    result(new SystemStream(SYSTEM, "bar")).getSystemStreamPartitionMetadata.size should equal(4)
    verify(systemAdmin, times(1)).getSystemStreamMetadata(Set("bar").asJava)
  }

  @Test
  def testCacheExpiry() {
    when(systemAdmin.getSystemStreamMetadata(Set("stream").asJava)).thenReturn(makeMetadata().asJava)
    val streams = Set(new SystemStream(SYSTEM, "stream"))

    when(clock.currentTimeMillis).thenReturn(0)
    streamMetadataCache.getStreamMetadata(streams)
    verify(systemAdmin, times(1)).getSystemStreamMetadata(Set("stream").asJava)

    when(clock.currentTimeMillis).thenReturn(streamMetadataCache.cacheTTLms / 2)
    streamMetadataCache.getStreamMetadata(streams)
    verify(systemAdmin, times(1)).getSystemStreamMetadata(Set("stream").asJava)

    when(clock.currentTimeMillis).thenReturn(2 * streamMetadataCache.cacheTTLms)
    streamMetadataCache.getStreamMetadata(streams)
    streamMetadataCache.getStreamMetadata(streams)
    streamMetadataCache.getStreamMetadata(streams)
    verify(systemAdmin, times(2)).getSystemStreamMetadata(Set("stream").asJava)
  }

  @Test
  def testGroupingRequestsBySystem() {
    when(systemAdmin.getSystemStreamMetadata(Set("stream1a", "stream1b").asJava))
      .thenReturn(makeMetadata(Set("stream1a", "stream1b"), numPartitions = 3).asJava)
    when(extendedSystemAdmin.getSystemStreamMetadata(Set("stream2a", "stream2b").asJava))
      .thenReturn(makeMetadata(Set("stream2a", "stream2b"), numPartitions = 5).asJava)
    val streams = Set(
      new SystemStream(SYSTEM, "stream1a"), new SystemStream(SYSTEM, "stream1b"),
      new SystemStream(EXTENDED_SYSTEM, "stream2a"), new SystemStream(EXTENDED_SYSTEM, "stream2b")
    )
    val result = streamMetadataCache.getStreamMetadata(streams)
    result.keySet shouldEqual streams
    streams.foreach(stream => {
      val expectedPartitions = if (stream.getSystem == SYSTEM) 3 else 5
      result(stream).getSystemStreamPartitionMetadata.size shouldEqual expectedPartitions
    })
    verify(systemAdmin, times(1)).getSystemStreamMetadata(Set("stream1a", "stream1b").asJava)
    verify(extendedSystemAdmin, times(1)).getSystemStreamMetadata(Set("stream2a", "stream2b").asJava)
  }

  @Test
  def testSystemOmitsStreamFromResult() {
    when(systemAdmin.getSystemStreamMetadata(Set("stream1", "stream2").asJava))
      .thenReturn(makeMetadata(Set("stream1")).asJava) // metadata doesn't include stream2
    val streams = Set(new SystemStream(SYSTEM, "stream1"), new SystemStream(SYSTEM, "stream2"))
    val exception = intercept[SamzaException] {
      streamMetadataCache.getStreamMetadata(streams)
    }
    exception.getMessage should startWith ("Cannot get metadata for unknown streams")
  }

  @Test
  def testSystemReturnsNullMetadata() {
    when(systemAdmin.getSystemStreamMetadata(Set("stream").asJava))
      .thenReturn(Map[String, SystemStreamMetadata]("stream" -> null).asJava)
    val streams = Set(new SystemStream(SYSTEM, "stream"))
    val exception = intercept[SamzaException] {
      streamMetadataCache.getStreamMetadata(streams)
    }
    exception.getMessage should startWith ("Cannot get metadata for unknown streams")
  }

  /**
    * Given that this is first time that a SystemStreamPartition has been queried, getNewestOffset should call the admin
    * to get the newest offset and return the result. It should also cache the data.
    */
  @Test
  def testGetNewestOffsetFirstFetch() {
    val ssp = new SystemStreamPartition(EXTENDED_SYSTEM, "stream", new Partition(0))
    // t = 10: first read, t = 11: first write, t = 11 + streamMetadataCache.cacheTTLms: second read (within TTL)
    when(clock.currentTimeMillis()).thenReturn(10, 11, 11 + streamMetadataCache.cacheTTLms)
    when(extendedSystemAdmin.getNewestOffsets(Set(ssp).asJava)).thenReturn(Map(ssp -> "5").asJava)
    assertEquals("5", streamMetadataCache.getNewestOffset(ssp))
    assertEquals("5", streamMetadataCache.getNewestOffset(ssp))
    verify(extendedSystemAdmin).getNewestOffsets(Set(ssp).asJava)
  }

  /**
    * Given that this is first time that a SystemStreamPartition has been queried and the SSP has no associated data
    * from the admin request, getNewestOffset should call the admin to get the newest offset and return null. It should
    * also cache the empty entry.
    */
  @Test
  def testGetNewestOffsetFirstFetchEmpty() {
    val ssp = new SystemStreamPartition(EXTENDED_SYSTEM, "stream", new Partition(0))
    // t = 10: first read, t = 11: first write, t = 11 + streamMetadataCache.cacheTTLms: second read (within TTL)
    when(clock.currentTimeMillis()).thenReturn(10, 11, 11 + streamMetadataCache.cacheTTLms)
    when(extendedSystemAdmin.getNewestOffsets(Set(ssp).asJava))
      .thenReturn(Map[SystemStreamPartition, String]().asJava)
    assertEquals(null, streamMetadataCache.getNewestOffset(ssp))
    assertEquals(null, streamMetadataCache.getNewestOffset(ssp))
    verify(extendedSystemAdmin).getNewestOffsets(Set(ssp).asJava)
  }

  /**
    * Given that SystemStreamPartition has been queried before and there is stale data in the cache for that SSP,
    * getNewestOffset should fetch the data.
    */
  @Test
  def testGetNewestOffsetStaleEntry() {
    val ssp = new SystemStreamPartition(EXTENDED_SYSTEM, "stream", new Partition(0))
    // t = 10: first read, t = 11: first write, t = 12 + streamMetadataCache.cacheTTLms: second read (outside TTL)
    when(clock.currentTimeMillis()).thenReturn(10, 11, 12 + streamMetadataCache.cacheTTLms)
    when(extendedSystemAdmin.getNewestOffsets(Set(ssp).asJava))
      .thenReturn(Map(ssp -> "5").asJava)
    assertEquals("5", streamMetadataCache.getNewestOffset(ssp))
    assertEquals("5", streamMetadataCache.getNewestOffset(ssp))
    verify(extendedSystemAdmin, times(2)).getNewestOffsets(Set(ssp).asJava)
  }

  /**
    * Given that SystemStreamPartition needs a fetch and there are previously queried SSPs in the cache, getNewestOffset
    * should fetch the data for the requested SSP and other SSPs with stale data with the same system. It should cache
    * all of the fetched data.
    */
  @Test
  def testGetNewestOffsetPrefetch() {
    // add one more extended system admin so we can have two of them for this test
    val otherExtendedSystemAdmin = mock[ExtendedSystemAdmin]
    val systemAdmins = Map(SYSTEM -> systemAdmin, EXTENDED_SYSTEM -> extendedSystemAdmin,
      "otherExtendedSystem" -> otherExtendedSystemAdmin)
    streamMetadataCache = new StreamMetadataCache(systemAdmins = new SystemAdmins(systemAdmins.asJava), clock = clock)

    val sspStale = new SystemStreamPartition(EXTENDED_SYSTEM, "stream", new Partition(0))
    // will have empty newest offset
    val sspStaleEmpty = new SystemStreamPartition(EXTENDED_SYSTEM, "stream", new Partition(1))
    // fresh entry should not get prefetched
    val sspFresh = new SystemStreamPartition(EXTENDED_SYSTEM, "stream", new Partition(2))
    // different system should not get prefetched
    val sspStaleDifferentSystem = new SystemStreamPartition("otherExtendedSystem", "otherStream", new Partition(2))

    // make some calls to fill in the cache for some ssps, and these will be used as "stale"
    when(clock.currentTimeMillis()).thenReturn(10)
    when(extendedSystemAdmin.getNewestOffsets(Set(sspStale).asJava))
      .thenReturn(Map(sspStale -> "10").asJava)
    when(extendedSystemAdmin.getNewestOffsets(Set(sspStaleEmpty).asJava))
      .thenReturn(Map[SystemStreamPartition, String]().asJava)
    when(otherExtendedSystemAdmin.getNewestOffsets(Set(sspStaleDifferentSystem).asJava))
      .thenReturn(Map(sspStaleDifferentSystem -> "11").asJava)
    streamMetadataCache.getNewestOffset(sspStale)
    streamMetadataCache.getNewestOffset(sspStaleEmpty)
    streamMetadataCache.getNewestOffset(sspStaleDifferentSystem)

    // add an entry which will be considered as "fresh"
    when(clock.currentTimeMillis()).thenReturn(100)
    when(extendedSystemAdmin.getNewestOffsets(Set(sspFresh).asJava))
      .thenReturn(Map(sspFresh -> "12").asJava)
    streamMetadataCache.getNewestOffset(sspFresh)

    // move clock forward so to trigger stale entries
    when(clock.currentTimeMillis()).thenReturn(11 + streamMetadataCache.cacheTTLms)
    when(extendedSystemAdmin.getNewestOffsets(Set(sspStale, sspStaleEmpty).asJava))
      .thenReturn(Map(sspStale -> "20").asJava)
    when(otherExtendedSystemAdmin.getNewestOffsets(Set(sspStaleDifferentSystem).asJava))
      .thenReturn(Map(sspStaleDifferentSystem -> "21").asJava)

    assertEquals("20", streamMetadataCache.getNewestOffset(sspStale))
    assertEquals(null, streamMetadataCache.getNewestOffset(sspStaleEmpty))
    assertEquals("21", streamMetadataCache.getNewestOffset(sspStaleDifferentSystem))
    assertEquals("12", streamMetadataCache.getNewestOffset(sspFresh))

    // should only get one admin call for sspStale individually (initial call)
    verify(extendedSystemAdmin).getNewestOffsets(Set(sspStale).asJava)
    // should only get one admin call for sspStaleEmpty individually (initial call)
    verify(extendedSystemAdmin).getNewestOffsets(Set(sspStaleEmpty).asJava)
    // should only get one admin call for sspFresh, since the data should be cached
    verify(extendedSystemAdmin).getNewestOffsets(Set(sspFresh).asJava)
    // should get two admin calls for sspStaleDifferentSystem (one initial, one after became stale)
    verify(otherExtendedSystemAdmin, times(2)).getNewestOffsets(Set(sspStaleDifferentSystem).asJava)
    /*
     * Should get one admin call for sspStale and sspStaleEmpty together which tests prefetch; should not include
     * sspStaleDifferentSystem since it is a different system.
     */
    verify(extendedSystemAdmin).getNewestOffsets(Set(sspStale, sspStaleEmpty).asJava)
  }

  /**
    * Given that there is only a SystemAdmin, getNewestOffset should fall back to getSystemStreamMetadata to get the
    * newest offset. It should still do caching.
    */
  @Test
  def testGetNewestOffsetSystemAdmin() {
    val ssp = new SystemStreamPartition(SYSTEM, "stream", new Partition(0))
    // t = 10: first read, t = 11: first write, t = 11 + streamMetadataCache.cacheTTLms: second read (within TTL)
    when(clock.currentTimeMillis()).thenReturn(10, 11, 11 + streamMetadataCache.cacheTTLms)
    when(systemAdmin.getSystemStreamMetadata(Set("stream").asJava))
      .thenReturn(Map("stream" -> new SystemStreamMetadata("stream",
        Map(new Partition(0) -> new SystemStreamPartitionMetadata("0", "10", "11")).asJava)).asJava)

    assertEquals("10", streamMetadataCache.getNewestOffset(ssp))
    assertEquals("10", streamMetadataCache.getNewestOffset(ssp))
    verify(systemAdmin).getSystemStreamMetadata(Set("stream").asJava)
  }

  /**
    * Given that there is only a SystemAdmin and no newest offset for a partition, getNewestOffset should return null.
    */
  @Test
  def testGetNewestOffsetSystemAdminNoNewestOffset() {
    val ssp = new SystemStreamPartition(SYSTEM, "stream", new Partition(0))
    // t = 10: first read, t = 11: first write, t = 11 + streamMetadataCache.cacheTTLms: second read (within TTL)
    when(clock.currentTimeMillis()).thenReturn(10, 11, 11 + streamMetadataCache.cacheTTLms)
    when(systemAdmin.getSystemStreamMetadata(Set("stream").asJava))
      .thenReturn(Map("stream" -> new SystemStreamMetadata("stream",
        Map(new Partition(0) -> new SystemStreamPartitionMetadata(null, null, null)).asJava)).asJava)

    assertEquals(null, streamMetadataCache.getNewestOffset(ssp))
    assertEquals(null, streamMetadataCache.getNewestOffset(ssp))
    verify(systemAdmin).getSystemStreamMetadata(Set("stream").asJava)
  }

  /**
    * Given that there is only a SystemAdmin and no metadata for a partition, getNewestOffset should return null.
    */
  @Test
  def testGetNewestOffsetSystemAdminNoPartitionMetadata(): Unit = {
    val ssp = new SystemStreamPartition(SYSTEM, "stream", new Partition(0))
    // t = 10: first read, t = 11: first write, t = 11 + streamMetadataCache.cacheTTLms: second read (within TTL)
    when(clock.currentTimeMillis()).thenReturn(10, 11, 11 + streamMetadataCache.cacheTTLms)
    when(systemAdmin.getSystemStreamMetadata(Set("stream").asJava))
      .thenReturn(Map("stream" -> new SystemStreamMetadata("stream",
        Map[Partition, SystemStreamPartitionMetadata]().asJava)).asJava)

    assertEquals(null, streamMetadataCache.getNewestOffset(ssp))
    assertEquals(null, streamMetadataCache.getNewestOffset(ssp))
    verify(systemAdmin).getSystemStreamMetadata(Set("stream").asJava)
  }

  /**
    * Given that there is only a SystemAdmin and no newest offset for a partition, getNewestOffset should return null.
    */
  @Test
  def testGetNewestOffsetSystemAdminNoStreamMetadata() {
    val ssp = new SystemStreamPartition(SYSTEM, "stream", new Partition(0))
    // t = 10: first read, t = 11: first write, t = 11 + streamMetadataCache.cacheTTLms: second read (within TTL)
    when(clock.currentTimeMillis()).thenReturn(10, 11, 11 + streamMetadataCache.cacheTTLms)
    when(systemAdmin.getSystemStreamMetadata(Set("stream").asJava))
      .thenReturn(Map[String, SystemStreamMetadata]().asJava)

    assertEquals(null, streamMetadataCache.getNewestOffset(ssp))
    assertEquals(null, streamMetadataCache.getNewestOffset(ssp))
    verify(systemAdmin).getSystemStreamMetadata(Set("stream").asJava)
  }

  /**
    * Given that there is only a SystemAdmin, getNewestOffset should still do prefetching.
    */
  @Test
  def testGetNewestOffsetSystemAdminPrefetch() {
    val ssp = new SystemStreamPartition(SYSTEM, "stream", new Partition(0))
    val sspOtherStream = new SystemStreamPartition(SYSTEM, "otherStream", new Partition(0))

    // fill in the cache
    when(clock.currentTimeMillis()).thenReturn(10)
    when(systemAdmin.getSystemStreamMetadata(Set("stream").asJava))
      .thenReturn(Map("stream" -> new SystemStreamMetadata("stream",
        Map(new Partition(0) -> new SystemStreamPartitionMetadata("0", "10", "11")).asJava)).asJava)
    when(systemAdmin.getSystemStreamMetadata(Set("otherStream").asJava))
      .thenReturn(Map("otherStream" -> new SystemStreamMetadata("otherStream",
        Map(new Partition(0) -> new SystemStreamPartitionMetadata("0", "12", "13")).asJava)).asJava)
    streamMetadataCache.getNewestOffset(ssp)
    streamMetadataCache.getNewestOffset(sspOtherStream)

    // move time forward to make the entries stale
    when(clock.currentTimeMillis()).thenReturn(11 + streamMetadataCache.cacheTTLms)
    when(systemAdmin.getSystemStreamMetadata(Set("stream", "otherStream").asJava)).thenReturn(
      Map("stream" -> new SystemStreamMetadata("stream",
        Map(new Partition(0) -> new SystemStreamPartitionMetadata("0", "20", "21")).asJava),
      "otherStream" -> new SystemStreamMetadata("otherStream",
        Map(new Partition(0) -> new SystemStreamPartitionMetadata("0", "22", "23")).asJava)).asJava)

    // get the newest offsets again; first call should do prefetching
    assertEquals("20", streamMetadataCache.getNewestOffset(ssp))
    assertEquals("22", streamMetadataCache.getNewestOffset(sspOtherStream))
    // only one call to each due to caching
    verify(systemAdmin).getSystemStreamMetadata(Set("stream").asJava)
    verify(systemAdmin).getSystemStreamMetadata(Set("otherStream").asJava)
    verify(systemAdmin).getSystemStreamMetadata(Set("stream", "otherStream").asJava)
  }

  /**
    * Given concurrent access to getNewestOffset, there should be only single calls to fetch metadata.
    */
  @Test
  def testGetNewestOffsetConcurrentAccess() {
    def ssp(i: Integer) = new SystemStreamPartition(EXTENDED_SYSTEM, "stream", new Partition(i))

    val numPartitions = 50
    // setup: fill the cache
    when(clock.currentTimeMillis()).thenReturn(10)
    Range(0, numPartitions).foreach(i => {
      when(extendedSystemAdmin.getNewestOffsets(Set(ssp(i)).asJava))
        .thenReturn(Map[SystemStreamPartition, String]().asJava)
      streamMetadataCache.getNewestOffset(ssp(i))
    })

    // move time forward to make the entries stale
    when(clock.currentTimeMillis()).thenReturn(11 + streamMetadataCache.cacheTTLms)

    // prefetching call
    when(extendedSystemAdmin.getNewestOffsets(Range(0, numPartitions).map(i => ssp(i)).toSet.asJava))
      .thenAnswer(new Answer[util.Map[SystemStreamPartition, String]] {
        override def answer(invocation: InvocationOnMock): util.Map[SystemStreamPartition, String] = {
          // wait a bit in order to have threads overlap on the lock
          Thread.sleep(1000)
          // use the partition id as the newest offset for uniqueness
          Range(0, numPartitions).map(i => ssp(i) -> i.toString).toMap.asJava
        }
      })
    // send concurrent requests for newest offsets
    val executorService = Executors.newFixedThreadPool(10)
    Range(0, numPartitions).foreach(i => {
      assertEquals(i.toString, executorService.submit(new Callable[String]() {
        override def call(): String = streamMetadataCache.getNewestOffset(ssp(i))
      }).get())
    })

    // should only see initial calls to fill cache and one call to get newest offsets from admin
    Range(0, numPartitions).foreach(i => {
      verify(extendedSystemAdmin).getNewestOffsets(Set(ssp(i)).asJava)
    })
    verify(extendedSystemAdmin).getNewestOffsets(Range(0, numPartitions).map(i => ssp(i)).toSet.asJava)
    verifyNoMoreInteractions(extendedSystemAdmin)

    executorService.shutdownNow()
  }

  /**
    * Given that the admin throws an exception when trying to get the newest offset, getNewestOffset should propagate
    * the exception.
    */
  @Test(expected = classOf[SamzaException])
  def testGetNewestOffsetException() {
    val ssp = new SystemStreamPartition(EXTENDED_SYSTEM, "stream", new Partition(0))
    when(extendedSystemAdmin.getNewestOffsets(Set(ssp).asJava)).thenThrow(new SamzaException())
    streamMetadataCache.getNewestOffset(ssp)
  }

  /**
    * Given that the a SystemAdmin throws an exception when trying to get the metadata, getNewestOffset should propagate
    * the exception.
    */
  @Test(expected = classOf[SamzaException])
  def testGetNewestOffsetSystemAdminException() {
    val ssp = new SystemStreamPartition(SYSTEM, "stream", new Partition(0))
    when(systemAdmin.getSystemStreamMetadata(Set("stream").asJava)).thenThrow(new SamzaException())
    streamMetadataCache.getNewestOffset(ssp)
  }
}