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

import org.apache.samza.{Partition, SamzaException}
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.util.Clock
import org.junit.{Before, Test}
import org.mockito.Mockito._
import org.scalatest.Matchers._
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConversions._

class TestStreamMetadataCache extends AssertionsForJUnit with MockitoSugar {
  def makeMetadata(streamNames: Set[String] = Set("stream"), numPartitions: Int = 4) = {
    val partitions = (0 until numPartitions).map(partition => {
      new Partition(partition) -> new SystemStreamPartitionMetadata("oldest", "newest", "upcoming")
    }).toMap
    streamNames.map(name => name -> new SystemStreamMetadata(name, partitions)).toMap
  }

  @Test
  def testFetchUncachedMetadataFromSystemAdmin {
    val systemAdmins = Map("foo" -> mock[SystemAdmin])
    when(systemAdmins("foo").getSystemStreamMetadata(Set("bar"))).thenReturn(makeMetadata(Set("bar")))
    val streams = Set(new SystemStream("foo", "bar"))
    val cache = new StreamMetadataCache(systemAdmins)

    val result = cache.getStreamMetadata(streams)
    streams shouldEqual result.keySet
    result(new SystemStream("foo", "bar")).getSystemStreamPartitionMetadata.size shouldBe 4
    verify(systemAdmins("foo"), times(1)).getSystemStreamMetadata(Set("bar"))
  }

  @Test
  def testCacheExpiry {
    val clock = mock[Clock]
    val systemAdmins = Map("system" -> mock[SystemAdmin])
    when(systemAdmins("system").getSystemStreamMetadata(Set("stream"))).thenReturn(makeMetadata())
    val streams = Set(new SystemStream("system", "stream"))
    val cache = new StreamMetadataCache(systemAdmins = systemAdmins, clock = clock)

    when(clock.currentTimeMillis).thenReturn(0)
    cache.getStreamMetadata(streams)
    verify(systemAdmins("system"), times(1)).getSystemStreamMetadata(Set("stream"))

    when(clock.currentTimeMillis).thenReturn(cache.cacheTTLms / 2)
    cache.getStreamMetadata(streams)
    verify(systemAdmins("system"), times(1)).getSystemStreamMetadata(Set("stream"))

    when(clock.currentTimeMillis).thenReturn(2 * cache.cacheTTLms)
    cache.getStreamMetadata(streams)
    cache.getStreamMetadata(streams)
    cache.getStreamMetadata(streams)
    verify(systemAdmins("system"), times(2)).getSystemStreamMetadata(Set("stream"))
  }

  @Test
  def testGroupingRequestsBySystem {
    val systemAdmins = Map("sys1" -> mock[SystemAdmin], "sys2" -> mock[SystemAdmin])
    when(systemAdmins("sys1").getSystemStreamMetadata(Set("stream1a", "stream1b")))
      .thenReturn(makeMetadata(Set("stream1a", "stream1b"), numPartitions = 3))
    when(systemAdmins("sys2").getSystemStreamMetadata(Set("stream2a", "stream2b")))
      .thenReturn(makeMetadata(Set("stream2a", "stream2b"), numPartitions = 5))
    val streams = Set(
      new SystemStream("sys1", "stream1a"), new SystemStream("sys1", "stream1b"),
      new SystemStream("sys2", "stream2a"), new SystemStream("sys2", "stream2b")
    )
    val result = new StreamMetadataCache(systemAdmins).getStreamMetadata(streams)
    result.keySet shouldEqual streams
    streams.foreach(stream => {
      val expectedPartitions = if (stream.getSystem == "sys1") 3 else 5
      result(stream).getSystemStreamPartitionMetadata.size shouldEqual expectedPartitions
    })
    verify(systemAdmins("sys1"), times(1)).getSystemStreamMetadata(Set("stream1a", "stream1b"))
    verify(systemAdmins("sys2"), times(1)).getSystemStreamMetadata(Set("stream2a", "stream2b"))
  }

  @Test
  def testSystemOmitsStreamFromResult {
    val systemAdmins = Map("system" -> mock[SystemAdmin])
    when(systemAdmins("system").getSystemStreamMetadata(Set("stream1", "stream2")))
      .thenReturn(makeMetadata(Set("stream1"))) // metadata doesn't include stream2
    val streams = Set(new SystemStream("system", "stream1"), new SystemStream("system", "stream2"))
    val exception = intercept[SamzaException] {
      new StreamMetadataCache(systemAdmins).getStreamMetadata(streams)
    }
    exception.getMessage should startWith ("Cannot get metadata for unknown streams")
  }

  @Test
  def testSystemReturnsNullMetadata {
    val systemAdmins = Map("system" -> mock[SystemAdmin])
    when(systemAdmins("system").getSystemStreamMetadata(Set("stream")))
      .thenReturn(Map("stream" -> null))
    val streams = Set(new SystemStream("system", "stream"))
    val exception = intercept[SamzaException] {
      new StreamMetadataCache(systemAdmins).getStreamMetadata(streams)
    }
    exception.getMessage should startWith ("Cannot get metadata for unknown streams")
  }
}
