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
