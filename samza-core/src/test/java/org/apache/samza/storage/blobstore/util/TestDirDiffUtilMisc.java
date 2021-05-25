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

import org.apache.samza.storage.blobstore.diff.DirDiff;
import org.apache.samza.storage.blobstore.index.DirIndex;

import java.io.IOException;
import java.nio.file.Path;
import java.util.SortedSet;
import java.util.TreeSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDirDiffUtilMisc {


  /**
   * Test the case when a file has been modified locally. I.e., when isSameFile returns false for a local file with
   * the same name as the remote file. The file should be marked for both deletion and addition.
   */
  @Test
  public void testGetDirDiffWhenIsSameFileReturnsFalseForSameFileName() throws IOException {
    String local = "[a]";
    String remote = "[a]";
    String expectedAdded = "[a]";
    String expectedRetained = "[]";
    String expectedRemoved = "[a]";

    Path localSnapshotDir = BlobStoreTestUtil.createLocalDir(local);
    String basePath = localSnapshotDir.toAbsolutePath().toString();
    DirIndex remoteSnapshotDir = BlobStoreTestUtil.createDirIndex(remote);

    // Execute
    DirDiff dirDiff = DirDiffUtil.getDirDiff(localSnapshotDir.toFile(), remoteSnapshotDir,
      (localFile, remoteFile) -> false);

    SortedSet<String> allAdded = new TreeSet<>();
    SortedSet<String> allRemoved = new TreeSet<>();
    SortedSet<String> allRetained = new TreeSet<>();
    BlobStoreTestUtil.getAllAddedInDiff(basePath, dirDiff, allAdded);
    BlobStoreTestUtil.getAllRemovedInDiff("", dirDiff, allRemoved);
    BlobStoreTestUtil.getAllRetainedInDiff("", dirDiff, allRetained);

    // Assert
    SortedSet<String> expectedAddedFiles = BlobStoreTestUtil.getExpected(expectedAdded);
    SortedSet<String> expectedRetainedFiles = BlobStoreTestUtil.getExpected(expectedRetained);
    SortedSet<String> expectedRemovedFiles = BlobStoreTestUtil.getExpected(expectedRemoved);
    assertEquals(expectedAddedFiles, allAdded);
    assertEquals(expectedRetainedFiles, allRetained);
    assertEquals(expectedRemovedFiles, allRemoved);
  }
}
