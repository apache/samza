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
package org.apache.samza.system;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestSSPMetadataCache {
  private static final String SYSTEM = "system";
  private static final String STREAM = "stream";
  private static final Duration CACHE_TTL = Duration.ofMillis(100);

  @Mock
  private SystemAdmin systemAdmin;
  @Mock
  private SystemAdmins systemAdmins;
  @Mock
  private Clock clock;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(systemAdmins.getSystemAdmin(SYSTEM)).thenReturn(systemAdmin);
  }

  /**
   * Given that there are sspsToPrefetch, getMetadata should call the admin (when necessary) to get the metadata for the
   * requested and "prefetch" SSPs. It should also cache the data.
   */
  @Test
  public void testGetMetadataWithPrefetch() {
    SystemStreamPartition ssp = buildSSP(0);
    SystemStreamPartition otherSSP = buildSSP(1);
    SSPMetadataCache cache = buildSSPMetadataCache(ImmutableSet.of(ssp, otherSSP));

    // t = 10: first read, t = 11: first write
    when(clock.currentTimeMillis()).thenReturn(10L, 11L);
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp, otherSSP))).thenReturn(
        ImmutableMap.of(ssp, sspMetadata(1), otherSSP, sspMetadata(2)));
    assertEquals(sspMetadata(1), cache.getMetadata(ssp));
    verify(systemAdmin).getSSPMetadata(ImmutableSet.of(ssp, otherSSP));

    // stay within TTL: use cached data
    when(clock.currentTimeMillis()).thenReturn(11 + CACHE_TTL.toMillis());
    assertEquals(sspMetadata(1), cache.getMetadata(ssp));
    assertEquals(sspMetadata(2), cache.getMetadata(otherSSP));
    // still only one call to the admin from the initial fill
    verify(systemAdmin).getSSPMetadata(ImmutableSet.of(ssp, otherSSP));

    // now entries are stale
    when(clock.currentTimeMillis()).thenReturn(12 + CACHE_TTL.toMillis());
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp, otherSSP))).thenReturn(
        ImmutableMap.of(ssp, sspMetadata(10), otherSSP, sspMetadata(11)));
    // flip the order; prefetching should still be done correctly
    assertEquals(sspMetadata(11), cache.getMetadata(otherSSP));
    assertEquals(sspMetadata(10), cache.getMetadata(ssp));
    verify(systemAdmin, times(2)).getSSPMetadata(ImmutableSet.of(ssp, otherSSP));
  }

  /**
   * Given that an SSP has empty metadata, getMetadata should return and cache that.
   */
  @Test
  public void testGetMetadataEmptyMetadata() {
    SystemStreamPartition ssp = buildSSP(0);
    SSPMetadataCache cache = buildSSPMetadataCache(ImmutableSet.of(ssp));

    // t = 10: first read, t = 11: first write
    when(clock.currentTimeMillis()).thenReturn(10L, 11L);
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of());
    assertNull(cache.getMetadata(ssp));
    verify(systemAdmin).getSSPMetadata(ImmutableSet.of(ssp));

    // stay within TTL: use cached data
    when(clock.currentTimeMillis()).thenReturn(11 + CACHE_TTL.toMillis());
    assertNull(cache.getMetadata(ssp));
    // still only one call to the admin from the initial fill
    verify(systemAdmin).getSSPMetadata(ImmutableSet.of(ssp));

    // now entries are stale
    when(clock.currentTimeMillis()).thenReturn(12 + CACHE_TTL.toMillis());
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of());
    assertNull(cache.getMetadata(ssp));
    verify(systemAdmin, times(2)).getSSPMetadata(ImmutableSet.of(ssp));
  }

  /**
   * Given that the there are sspsToPrefetch with systems that do not match the requested SSP, getMetadata should not
   * prefetch all sspsToPrefetch.
   */
  @Test
  public void testGetMetadataMultipleSystemsForPrefetch() {
    // add one more extended system admin so we can have two of them for this test
    SystemAdmin otherSystemAdmin = mock(SystemAdmin.class);
    String otherSystem = "otherSystem";
    when(systemAdmins.getSystemAdmin(otherSystem)).thenReturn(otherSystemAdmin);
    SystemStreamPartition ssp = buildSSP(0);
    // different system should not get prefetched
    SystemStreamPartition sspOtherSystem = new SystemStreamPartition(otherSystem, "otherStream", new Partition(1));
    SSPMetadataCache cache = buildSSPMetadataCache(ImmutableSet.of(ssp, sspOtherSystem));

    // t = 10: first read for ssp, t = 11: first write for ssp
    when(clock.currentTimeMillis()).thenReturn(10L, 11L);
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of(ssp, sspMetadata(1)));
    assertEquals(sspMetadata(1), cache.getMetadata(ssp));
    // does not call for sspOtherSystem
    verify(systemAdmin).getSSPMetadata(ImmutableSet.of(ssp));

    // t = 12: first read for sspOtherSystem, t = 13: first write for sspOtherSystem
    when(clock.currentTimeMillis()).thenReturn(12L, 13L);
    when(otherSystemAdmin.getSSPMetadata(ImmutableSet.of(sspOtherSystem))).thenReturn(
        ImmutableMap.of(sspOtherSystem, sspMetadata(2)));
    assertEquals(sspMetadata(2), cache.getMetadata(sspOtherSystem));
    // does not call for ssp
    verify(otherSystemAdmin).getSSPMetadata(ImmutableSet.of(sspOtherSystem));

    // now entries are stale, do another round of individual fetches
    when(clock.currentTimeMillis()).thenReturn(14 + CACHE_TTL.toMillis());
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of(ssp, sspMetadata(10)));
    assertEquals(sspMetadata(10), cache.getMetadata(ssp));
    verify(systemAdmin, times(2)).getSSPMetadata(ImmutableSet.of(ssp));
    when(otherSystemAdmin.getSSPMetadata(ImmutableSet.of(sspOtherSystem))).thenReturn(
        ImmutableMap.of(sspOtherSystem, sspMetadata(11)));
    assertEquals(sspMetadata(11), cache.getMetadata(sspOtherSystem));
    verify(otherSystemAdmin, times(2)).getSSPMetadata(ImmutableSet.of(sspOtherSystem));
  }

  /**
   * Given that there are no sspsToPrefetch, getMetadata should still fetch and cache metadata for a requested SSP.
   */
  @Test
  public void testGetMetadataNoSSPsToPrefetch() {
    SystemStreamPartition ssp = buildSSP(0);
    SSPMetadataCache cache = buildSSPMetadataCache(ImmutableSet.of());

    // t = 10: first read, t = 11: first write
    when(clock.currentTimeMillis()).thenReturn(10L, 11L);
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of(ssp, sspMetadata(1)));
    assertEquals(sspMetadata(1), cache.getMetadata(ssp));
    verify(systemAdmin).getSSPMetadata(ImmutableSet.of(ssp));

    // stay within TTL: use cached data
    when(clock.currentTimeMillis()).thenReturn(11 + CACHE_TTL.toMillis());
    assertEquals(sspMetadata(1), cache.getMetadata(ssp));

    // now entry is stale
    when(clock.currentTimeMillis()).thenReturn(12 + CACHE_TTL.toMillis());
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of(ssp, sspMetadata(10)));
    assertEquals(sspMetadata(10), cache.getMetadata(ssp));
    verify(systemAdmin, times(2)).getSSPMetadata(ImmutableSet.of(ssp));
  }

  /**
   * Given that the sspsToPrefetch does not contain the requested SSP, getMetadata should still fetch and cache metadata
   * for it.
   */
  @Test
  public void testGetMetadataRequestedSSPNotInSSPsToPrefetch() {
    SystemStreamPartition ssp = buildSSP(0);
    SystemStreamPartition otherSSP = buildSSP(1);
    // do not include ssp in sspsToPrefetch
    SSPMetadataCache cache = buildSSPMetadataCache(ImmutableSet.of(otherSSP));

    // t = 10: first read, t = 11: first write
    when(clock.currentTimeMillis()).thenReturn(10L, 11L);
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp, otherSSP))).thenReturn(
        ImmutableMap.of(ssp, sspMetadata(1), otherSSP, sspMetadata(2)));
    assertEquals(sspMetadata(1), cache.getMetadata(ssp));
    // still will fetch metadata for both
    verify(systemAdmin).getSSPMetadata(ImmutableSet.of(ssp, otherSSP));

    // stay within TTL: use cached data
    when(clock.currentTimeMillis()).thenReturn(11 + CACHE_TTL.toMillis());
    assertEquals(sspMetadata(1), cache.getMetadata(ssp));
    assertEquals(sspMetadata(2), cache.getMetadata(otherSSP));
    // still only one call to the admin from the initial fill
    verify(systemAdmin).getSSPMetadata(ImmutableSet.of(ssp, otherSSP));

    // now entries are stale
    when(clock.currentTimeMillis()).thenReturn(12 + CACHE_TTL.toMillis());
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(
        ImmutableMap.of(ssp, sspMetadata(10), otherSSP, sspMetadata(11)));
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(otherSSP))).thenReturn(
        ImmutableMap.of(otherSSP, sspMetadata(11)));
    // call for otherSSP first; no prefetching since ssp is not in sspsToPrefetch
    assertEquals(sspMetadata(11), cache.getMetadata(otherSSP));
    verify(systemAdmin).getSSPMetadata(ImmutableSet.of(otherSSP));
    // call for ssp also has no prefetching since the otherSSP metadata is fresh at this point
    assertEquals(sspMetadata(10), cache.getMetadata(ssp));
    verify(systemAdmin).getSSPMetadata(ImmutableSet.of(ssp));
    // still only one call for both at the same time from the initial fill
    verify(systemAdmin).getSSPMetadata(ImmutableSet.of(ssp, otherSSP));
  }

  /**
   * Given concurrent access to getMetadata, there should be only single calls to fetch metadata.
   */
  @Test
  public void testGetMetadataConcurrentAccess() throws ExecutionException, InterruptedException {
    int numPartitions = 50;
    // initial fetch
    when(clock.currentTimeMillis()).thenReturn(10L);
    Set<SystemStreamPartition> ssps =
        IntStream.range(0, numPartitions).mapToObj(TestSSPMetadataCache::buildSSP).collect(Collectors.toSet());
    SSPMetadataCache cache = buildSSPMetadataCache(ssps);
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    when(systemAdmin.getSSPMetadata(ssps)).thenAnswer(invocation -> {
        // have the admin call wait so that it forces the threads to overlap on the lock
        Thread.sleep(500);
        return IntStream.range(0, numPartitions)
            .boxed()
            .collect(Collectors.toMap(TestSSPMetadataCache::buildSSP, i -> sspMetadata((long) i)));
      });

    // send concurrent requests for metadata
    List<Future<SystemStreamMetadata.SystemStreamPartitionMetadata>> getMetadataFutures =
        IntStream.range(0, numPartitions)
            .mapToObj(i -> executorService.submit(() -> cache.getMetadata(buildSSP(i))))
            .collect(Collectors.toList());
    for (int i = 0; i < numPartitions; i++) {
      assertEquals(sspMetadata(i), getMetadataFutures.get(i).get());
    }
    // should only see one call to fetch metadata
    verify(systemAdmin).getSSPMetadata(ssps);

    // make entries stale
    when(clock.currentTimeMillis()).thenReturn(11 + CACHE_TTL.toMillis());
    getMetadataFutures = IntStream.range(0, numPartitions)
        .mapToObj(i -> executorService.submit(() -> cache.getMetadata(buildSSP(i))))
        .collect(Collectors.toList());
    for (int i = 0; i < numPartitions; i++) {
      assertEquals(sspMetadata(i), getMetadataFutures.get(i).get());
    }
    // should see two total calls to fetch metadata
    verify(systemAdmin, times(2)).getSSPMetadata(ssps);
  }

  /**
   * Given that the admin throws an exception when trying to get the metadata for the first time, getMetadata should
   * propagate the exception.
   */
  @Test(expected = SamzaException.class)
  public void testGetMetadataExceptionFirstFetch() {
    SystemStreamPartition ssp = buildSSP(0);
    SSPMetadataCache cache = buildSSPMetadataCache(ImmutableSet.of(ssp));
    when(clock.currentTimeMillis()).thenReturn(10L);
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenThrow(new SamzaException());
    cache.getMetadata(ssp);
  }

  /**
   * Given that the admin throws an exception when trying to get the metadata after a successful fetch, getMetadata
   * should propagate the exception.
   */
  @Test(expected = SamzaException.class)
  public void testGetMetadataExceptionAfterSuccessfulFetch() {
    SystemStreamPartition ssp = buildSSP(0);
    SSPMetadataCache cache = buildSSPMetadataCache(ImmutableSet.of(ssp));

    // do a successful fetch first
    when(clock.currentTimeMillis()).thenReturn(10L);
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of(ssp, sspMetadata(1)));
    cache.getMetadata(ssp);

    // throw an exception on the next fetch
    when(clock.currentTimeMillis()).thenReturn(11 + CACHE_TTL.toMillis());
    when(systemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenThrow(new SamzaException());
    cache.getMetadata(ssp);
  }

  private SSPMetadataCache buildSSPMetadataCache(Set<SystemStreamPartition> sspsToPrefetch) {
    return new SSPMetadataCache(systemAdmins, CACHE_TTL, clock, sspsToPrefetch);
  }

  private static SystemStreamPartition buildSSP(int partition) {
    return new SystemStreamPartition(SYSTEM, STREAM, new Partition(partition));
  }

  private static SystemStreamMetadata.SystemStreamPartitionMetadata sspMetadata(long baseOffset) {
    return new SystemStreamMetadata.SystemStreamPartitionMetadata(Long.toString(baseOffset),
        Long.toString(baseOffset * 100), Long.toString(baseOffset * 100 + 1));
  }
}