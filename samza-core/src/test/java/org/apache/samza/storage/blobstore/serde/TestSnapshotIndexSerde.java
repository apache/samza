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

package org.apache.samza.storage.blobstore.serde;

import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;
import org.apache.samza.storage.blobstore.index.serde.SnapshotIndexSerde;
import org.apache.samza.storage.blobstore.util.BlobStoreTestUtil;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.samza.checkpoint.CheckpointId;
import org.junit.Assert;
import org.junit.Test;


public class TestSnapshotIndexSerde {
  @Test
  public void testSnapshotIndexSerde() throws IOException {
    // create local and remote snapshots
    String local = "[a, b, c/1, d/1/2]";
    String remote = "[a, b, z, c/1/2, e/1]";

    Path localSnapshot = BlobStoreTestUtil.createLocalDir(local);
    DirIndex dirIndex = BlobStoreTestUtil.createDirIndex(remote);
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(CheckpointId.create(), "job", "123", "task", "store");
    SnapshotIndex testRemoteSnapshot =
        new SnapshotIndex(System.currentTimeMillis(), snapshotMetadata, dirIndex, Optional.empty());

    SnapshotIndexSerde snapshotIndexSerde = new SnapshotIndexSerde();
    byte[] serialized = snapshotIndexSerde.toBytes(testRemoteSnapshot);
    SnapshotIndex deserialized = snapshotIndexSerde.fromBytes(serialized);

    Assert.assertNotNull(deserialized);
    Assert.assertEquals(deserialized, testRemoteSnapshot);
  }
}
