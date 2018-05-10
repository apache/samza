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

import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.util.Clock
import org.apache.samza.{Partition, SamzaException}
import org.junit.{Before, Test}
import org.mockito.Mockito._
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers => ScalaTestMatchers}

import scala.collection.JavaConverters._

class TestStreamMetadataCache extends AssertionsForJUnit with MockitoSugar with ScalaTestMatchers {
  private val SYSTEM = "system"
  private val OTHER_SYSTEM = "otherSystem"
  private val cacheTTL = 500

  @Mock
  var systemAdmin: SystemAdmin = _
  @Mock
  var otherSystemAdmin: SystemAdmin = _
  @Mock
  var systemAdmins: SystemAdmins = _
  @Mock
  var clock: Clock = _
  var cache: StreamMetadataCache = _

  @Before
  def setup(): Unit = {
    MockitoAnnotations.initMocks(this)
    when(systemAdmins.getSystemAdmin(SYSTEM)).thenReturn(systemAdmin)
    when(systemAdmins.getSystemAdmin(OTHER_SYSTEM)).thenReturn(otherSystemAdmin)
    cache = new StreamMetadataCache(systemAdmins, cacheTTL, clock)
  }

  private def makeMetadata(streamNames: Set[String] = Set("stream"), numPartitions: Int = 4): Map[String, SystemStreamMetadata] = {
    val partitions = (0 until numPartitions).map(partition => {
      new Partition(partition) -> new SystemStreamPartitionMetadata("oldest", "newest", "upcoming")
    }).toMap
    streamNames.map(name => name -> new SystemStreamMetadata(name, partitions.asJava)).toMap
  }

  @Test
  def testFetchUncachedMetadataFromSystemAdmin() {
    when(systemAdmin.getSystemStreamMetadata(Set("bar").asJava)).thenReturn(makeMetadata(Set("bar")).asJava)
    val streams = Set(new SystemStream(SYSTEM, "bar"))
    val result = cache.getStreamMetadata(streams)
    streams shouldEqual result.keySet
    result(new SystemStream(SYSTEM, "bar")).getSystemStreamPartitionMetadata.size should equal(4)
    verify(systemAdmin).getSystemStreamMetadata(Set("bar").asJava)
  }

  @Test
  def testCacheExpiry() {
    when(systemAdmin.getSystemStreamMetadata(Set("stream").asJava)).thenReturn(makeMetadata().asJava)
    val streams = Set(new SystemStream(SYSTEM, "stream"))

    when(clock.currentTimeMillis).thenReturn(0)
    cache.getStreamMetadata(streams)
    verify(systemAdmin).getSystemStreamMetadata(Set("stream").asJava)

    when(clock.currentTimeMillis).thenReturn(cacheTTL / 2)
    cache.getStreamMetadata(streams)
    verify(systemAdmin).getSystemStreamMetadata(Set("stream").asJava)

    when(clock.currentTimeMillis).thenReturn(2 * cacheTTL)
    cache.getStreamMetadata(streams)
    cache.getStreamMetadata(streams)
    cache.getStreamMetadata(streams)
    verify(systemAdmin, times(2)).getSystemStreamMetadata(Set("stream").asJava)
  }

  @Test
  def testGroupingRequestsBySystem() {
    when(systemAdmin.getSystemStreamMetadata(Set("stream1a", "stream1b").asJava))
      .thenReturn(makeMetadata(Set("stream1a", "stream1b"), numPartitions = 3).asJava)
    when(otherSystemAdmin.getSystemStreamMetadata(Set("stream2a", "stream2b").asJava))
      .thenReturn(makeMetadata(Set("stream2a", "stream2b"), numPartitions = 5).asJava)
    val streams = Set(
      new SystemStream(SYSTEM, "stream1a"), new SystemStream(SYSTEM, "stream1b"),
      new SystemStream(OTHER_SYSTEM, "stream2a"), new SystemStream(OTHER_SYSTEM, "stream2b")
    )
    val result = cache.getStreamMetadata(streams)
    result.keySet shouldEqual streams
    streams.foreach(stream => {
      val expectedPartitions = if (stream.getSystem == SYSTEM) 3 else 5
      result(stream).getSystemStreamPartitionMetadata.size shouldEqual expectedPartitions
    })
    verify(systemAdmin).getSystemStreamMetadata(Set("stream1a", "stream1b").asJava)
    verify(otherSystemAdmin).getSystemStreamMetadata(Set("stream2a", "stream2b").asJava)
  }

  @Test
  def testSystemOmitsStreamFromResult() {
    when(systemAdmin.getSystemStreamMetadata(Set("stream1", "stream2").asJava))
      .thenReturn(makeMetadata(Set("stream1")).asJava) // metadata doesn't include stream2
    val streams = Set(new SystemStream(SYSTEM, "stream1"), new SystemStream(SYSTEM, "stream2"))
    val exception = intercept[SamzaException] {
      cache.getStreamMetadata(streams)
    }
    exception.getMessage should startWith ("Cannot get metadata for unknown streams")
  }

  @Test
  def testSystemReturnsNullMetadata() {
    when(systemAdmin.getSystemStreamMetadata(Set("stream").asJava))
      .thenReturn(Map[String, SystemStreamMetadata]("stream" -> null).asJava)
    val streams = Set(new SystemStream(SYSTEM, "stream"))
    val exception = intercept[SamzaException] {
      cache.getStreamMetadata(streams)
    }
    exception.getMessage should startWith ("Cannot get metadata for unknown streams")
  }
}
