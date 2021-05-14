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

package org.apache.samza.storage.blobstore.util;

import com.google.common.collect.ImmutableMap;
import org.apache.samza.storage.blobstore.BlobStoreStateBackendFactory;
import org.apache.samza.storage.blobstore.Metadata;
import org.apache.samza.storage.blobstore.exceptions.DeletedException;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.util.FutureUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestBlobStoreStateBackendUtil {

  @Test
  public void testGetSSIReturnsEmptyMapForNullCheckpoint() {
    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);
    Map<String, Pair<String, SnapshotIndex>> snapshotIndexes =
        BlobStoreStateBackendUtil.getStoreSnapshotIndexes("testJobName", "testJobId","taskName", null, mockBlobStoreUtil);
    assertTrue(snapshotIndexes.isEmpty());
  }

  @Test(expected = SamzaException.class)
  public void testGetSSIThrowsExceptionForCheckpointV1() {
    Checkpoint mockCheckpoint = mock(Checkpoint.class);
    when(mockCheckpoint.getVersion()).thenReturn((short) 1);
    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);
    BlobStoreStateBackendUtil.getStoreSnapshotIndexes("testJobName", "testJobId","taskName", mockCheckpoint, mockBlobStoreUtil);
  }

  @Test
  public void testGetSSIReturnsEmptyMapIfNoEntryForBlobStoreBackendFactory() {
    CheckpointV2 mockCheckpoint = mock(CheckpointV2.class);
    when(mockCheckpoint.getVersion()).thenReturn((short) 2);
    when(mockCheckpoint.getStateCheckpointMarkers())
        .thenReturn(ImmutableMap.of("com.OtherStateBackendFactory", ImmutableMap.of("storeName", "otherSCM")));

    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);
    Map<String, Pair<String, SnapshotIndex>> snapshotIndexes =
        BlobStoreStateBackendUtil.getStoreSnapshotIndexes("testJobName", "testJobId","taskName", mockCheckpoint, mockBlobStoreUtil);
    assertTrue(snapshotIndexes.isEmpty());
  }

  @Test
  public void testGetSSIReturnsEmptyMapIfNoStoreForBlobStoreBackendFactory() {
    CheckpointV2 mockCheckpoint = mock(CheckpointV2.class);
    when(mockCheckpoint.getVersion()).thenReturn((short) 2);
    when(mockCheckpoint.getStateCheckpointMarkers())
        .thenReturn(ImmutableMap.of(BlobStoreStateBackendFactory.class.getName(), ImmutableMap.of()));

    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);
    Map<String, Pair<String, SnapshotIndex>> snapshotIndexes =
        BlobStoreStateBackendUtil.getStoreSnapshotIndexes("testJobName", "testJobId","taskName", mockCheckpoint, mockBlobStoreUtil);
    assertTrue(snapshotIndexes.isEmpty());
  }

  @Test(expected = SamzaException.class)
  public void testGetSSIThrowsExceptionOnSyncBlobStoreErrors() {
    Checkpoint checkpoint = createCheckpointV2(BlobStoreStateBackendFactory.class.getName(),
        ImmutableMap.of("storeName", "snapshotIndexBlobId"));
    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);
    when(mockBlobStoreUtil.getSnapshotIndex(anyString(), any(Metadata.class))).thenThrow(new RuntimeException());
    BlobStoreStateBackendUtil.getStoreSnapshotIndexes("testJobName", "testJobId","taskName", checkpoint, mockBlobStoreUtil);
  }

  @Test
  public void testGetSSISkipsStoresWithSnapshotIndexAlreadyDeleted() {
    Checkpoint checkpoint = createCheckpointV2(BlobStoreStateBackendFactory.class.getName(),
        ImmutableMap.of(
            "storeName1", "snapshotIndexBlobId1",
            "storeName2", "snapshotIndexBlobId2"
        ));
    SnapshotIndex store1SnapshotIndex = mock(SnapshotIndex.class);
    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);
    CompletableFuture<SnapshotIndex> failedFuture = FutureUtil.failedFuture(new DeletedException());
    when(mockBlobStoreUtil.getSnapshotIndex(eq("snapshotIndexBlobId1"), any(Metadata.class)))
        .thenReturn(CompletableFuture.completedFuture(store1SnapshotIndex));
    when(mockBlobStoreUtil.getSnapshotIndex(eq("snapshotIndexBlobId2"), any(Metadata.class)))
        .thenReturn(failedFuture);

    Map<String, Pair<String, SnapshotIndex>> snapshotIndexes =
        BlobStoreStateBackendUtil.getStoreSnapshotIndexes("testJobName", "testJobId","taskName", checkpoint, mockBlobStoreUtil);
    assertEquals(1, snapshotIndexes.size());
    assertEquals("snapshotIndexBlobId1", snapshotIndexes.get("storeName1").getLeft());
    assertEquals(store1SnapshotIndex, snapshotIndexes.get("storeName1").getRight());
  }

  @Test
  public void testGetSSIThrowsExceptionIfAnyNonIgnoredAsyncBlobStoreErrors() {
    Checkpoint checkpoint = createCheckpointV2(BlobStoreStateBackendFactory.class.getName(),
        ImmutableMap.of(
            "storeName1", "snapshotIndexBlobId1",
            "storeName2", "snapshotIndexBlobId2"
        ));
    SnapshotIndex store1SnapshotIndex = mock(SnapshotIndex.class);
    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);
    RuntimeException nonIgnoredException = new RuntimeException();
    CompletableFuture<SnapshotIndex> failedFuture = FutureUtil.failedFuture(nonIgnoredException);
    when(mockBlobStoreUtil.getSnapshotIndex(eq("snapshotIndexBlobId1"), any(Metadata.class)))
        .thenReturn(FutureUtil.failedFuture(new DeletedException())); // should fail even if some errors are ignored
    when(mockBlobStoreUtil.getSnapshotIndex(eq("snapshotIndexBlobId2"), any(Metadata.class)))
        .thenReturn(failedFuture);

    try {
      BlobStoreStateBackendUtil.getStoreSnapshotIndexes("testJobName", "testJobId","taskName", checkpoint, mockBlobStoreUtil);
      fail("Should have thrown an exception");
    } catch (Exception e) {
      Throwable cause = FutureUtil.unwrapExceptions(CompletionException.class,
          FutureUtil.unwrapExceptions(SamzaException.class, e));
      assertEquals(nonIgnoredException, cause);
    }
  }

  @Test
  public void testGetSSIReturnsCorrectSCMSnapshotIndexPair() {
    String storeName = "storeName";
    String otherStoreName = "otherStoreName";
    String storeSnapshotIndexBlobId = "snapshotIndexBlobId";
    String otherStoreSnapshotIndexBlobId = "otherSnapshotIndexBlobId";
    SnapshotIndex mockStoreSnapshotIndex = mock(SnapshotIndex.class);
    SnapshotIndex mockOtherStooreSnapshotIndex = mock(SnapshotIndex.class);

    CheckpointV2 checkpoint = createCheckpointV2(BlobStoreStateBackendFactory.class.getName(),
        ImmutableMap.of(storeName, storeSnapshotIndexBlobId, otherStoreName, otherStoreSnapshotIndexBlobId));

    BlobStoreUtil mockBlobStoreUtil = mock(BlobStoreUtil.class);

    when(mockBlobStoreUtil.getSnapshotIndex(eq(storeSnapshotIndexBlobId), any(Metadata.class)))
        .thenReturn(CompletableFuture.completedFuture(mockStoreSnapshotIndex));
    when(mockBlobStoreUtil.getSnapshotIndex(eq(otherStoreSnapshotIndexBlobId), any(Metadata.class)))
        .thenReturn(CompletableFuture.completedFuture(mockOtherStooreSnapshotIndex));

    Map<String, Pair<String, SnapshotIndex>> snapshotIndexes =
        BlobStoreStateBackendUtil.getStoreSnapshotIndexes("testJobName", "testJobId","taskName", checkpoint, mockBlobStoreUtil);

    assertEquals(storeSnapshotIndexBlobId, snapshotIndexes.get(storeName).getKey());
    assertEquals(mockStoreSnapshotIndex, snapshotIndexes.get(storeName).getValue());
    assertEquals(otherStoreSnapshotIndexBlobId, snapshotIndexes.get(otherStoreName).getKey());
    assertEquals(mockOtherStooreSnapshotIndex, snapshotIndexes.get(otherStoreName).getValue());
    verify(mockBlobStoreUtil, times(2)).getSnapshotIndex(anyString(), any(Metadata.class));
  }

  private CheckpointV2 createCheckpointV2(String stateBackendFactory, Map<String, String> storeSnapshotIndexBlobIds) {
    CheckpointId checkpointId = CheckpointId.create();
    Map<String, Map<String, String>> factoryStoreSCMs = new HashMap<>();
    Map<String, String> storeSCMs = new HashMap<>();
    for (Map.Entry<String, String> entry: storeSnapshotIndexBlobIds.entrySet()) {
      storeSCMs.put(entry.getKey(), entry.getValue());
    }

    factoryStoreSCMs.put(stateBackendFactory, storeSCMs);
    return new CheckpointV2(checkpointId, ImmutableMap.of(), factoryStoreSCMs);
  }
}
