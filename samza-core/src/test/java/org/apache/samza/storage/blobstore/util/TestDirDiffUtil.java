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
import java.util.Arrays;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;


@RunWith(value = Parameterized.class)
public class TestDirDiffUtil {

  private final String local;
  private final String remote;
  private final String expectedAdded;
  private final String expectedRetained;
  private final String expectedRemoved;
  private final String description;

  public TestDirDiffUtil(String local, String remote,
      String expectedAdded, String expectedRetained, String expectedRemoved,
      String description) {
    this.local = local;
    this.remote = remote;
    this.expectedAdded = expectedAdded;
    this.expectedRetained = expectedRetained;
    this.expectedRemoved = expectedRemoved;
    this.description = description;
  }

  // TODO HIGH shesharm test with empty subdirectories
  @Parameterized.Parameters(name = "testGetDirDiff: {5}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        // Local                             Remote                             Expected Added        Expected Retained    Expected Removed     Description
        // -------------------------------- --------------------------------    -----------------     ------------------   -----------------    ------------------------------------------------------------------
        {"[]",                               "[]",                               "[]",                "[]",                "[]",                "Nothing in local or remote"},
        {"[a]",                              "[]",                               "[a]",               "[]",                "[]",                "New file in local"},
        {"[z/1]",                            "[]",                               "[z/1]",             "[]",                "[]",                "New dir in local"},
        {"[z/i/1]",                          "[]",                               "[z/i/1]",           "[]",                "[]",                "New recursive dir in local"},
        {"[a, z/1]",                         "[]",                               "[a, z/1]",          "[]",                "[]",                "New file and dir in local"},
        {"[a, z/1, y/j/1]",                  "[]",                               "[a, z/1, y/j/1]",   "[]",                "[]",                "New file, dir and recursive dir in local"},
        {"[a]",                              "[a]",                              "[]",                "[a]",               "[]",                "File retained in local"},
        {"[z/1]",                            "[z/1]",                            "[]",                "[z/1]",             "[]",                "Dir retained in local"},
        {"[z/i/1]",                          "[z/i/1]",                          "[]",                "[z/i/1]",           "[]",                "Recursive dir retained in local"},
        {"[a, z/1]",                         "[a, z/1]",                         "[]",                "[a, z/1]",          "[]",                "File and dir retained in local"},
        {"[a, z/1, y/j/1]",                  "[a, z/1, y/j/1]",                  "[]",                "[a, z/1, y/j/1]",   "[]",                "File, dir and recursive dir retained in local"},
        {"[]",                               "[a]",                              "[]",                "[]",                "[a]",               "File removed in local"},
        {"[]",                               "[z/1]",                            "[]",                "[]",                "[z/1]",             "Dir removed in local"},
        {"[]",                               "[z/i/1]",                          "[]",                "[]",                "[z/i/1]",           "Recursive dir removed in local"},
        {"[]",                               "[a, z/1]",                         "[]",                "[]",                "[a, z/1]",          "File and dir removed in local"},
        {"[]",                               "[a, z/1, y/j/1]",                  "[]",                "[]",                "[a, z/1, y/j/1]",   "File, dir and recursive dir removed in local"},
        {"[b]",                              "[a]",                              "[b]",               "[]",                "[a]",               "File added and removed in local"},
        {"[y/1]",                            "[z/1]",                            "[y/1]",             "[]",                "[z/1]",             "Dir added and removed in local"},
        {"[y/j/1]",                          "[z/i/1]",                          "[y/j/1]",           "[]",                "[z/i/1]",           "Recursive dir added and removed in local"},
        {"[b, y/1]",                         "[a, z/1]",                         "[b, y/1]",          "[]",                "[a, z/1]",          "File and dir added and removed in local"},
        {"[b, y/1, x/k/1]",                  "[a, z/1, w/m/1]",                  "[b, y/1, x/k/1]",   "[]",                "[a, z/1, w/m/1]",   "File, dir and recursive dir added and removed in local"},
        {"[a, c]",                           "[a]",                              "[c]",               "[a]",               "[]",                "File added and retained in local"},
        {"[z/1, y/1]",                       "[z/1]",                            "[y/1]",             "[z/1]",             "[]",                "Dir added and retained in local"},
        {"[z/i/1, y/j/1]",                   "[z/i/1]",                          "[y/j/1]",           "[z/i/1]",           "[]",                "Recursive dir added and retained in local"},
        {"[a, c, z/1, y/1]",                 "[a, z/1]",                         "[c, y/1]",          "[a, z/1]",          "[]",                "File and dir added and retained in local"},
        {"[a, c, z/1, y/1, p/m/1, q/n/1]",   "[a, z/1, p/m/1]",                  "[c, y/1, q/n/1]",   "[a, z/1, p/m/1]",   "[]",                "File, dir and recursive dir added and retained in local"},
        {"[a, c]",                           "[a, b]",                           "[c]",               "[a]",               "[b]",               "File added, retained and removed in local"},
        {"[z/1, y/1]",                       "[z/1, x/1]",                       "[y/1]",             "[z/1]",             "[x/1]",             "Dir added, retained and removed in local"},
        {"[z/1, z/3]",                       "[z/1, z/2]",                       "[z/3]",             "[z/1]",             "[z/2]",             "File added, retained and removed in dir in local"},
        {"[z/i/1, y/j/1]",                   "[z/i/1, x/k/1]",                   "[y/j/1]",           "[z/i/1]",           "[x/k/1]",           "Recursive dir added, retained and removed in local"},
        {"[a, c, z/1, y/1]",                 "[a, b, z/1, x/1]",                 "[c, y/1]",          "[a, z/1]",          "[b, x/1]",          "File and dir added, retained and removed in local"},
        {"[a, c, z/1, y/1, p/m/1, q/n/1]",   "[a, b, z/1, x/1, p/m/1, r/o/1]",   "[c, y/1, q/n/1]",   "[a, z/1, p/m/1]",   "[b, x/1, r/o/1]",   "File, dir and recursive dir added, retained and removed in local"},
        {"[a, c, z/1, p/m/1, p/m/2, q/n/1]", "[a, b, z/1, x/1, p/m/1, r/o/1]",   "[c, p/m/2, q/n/1]", "[a, z/1, p/m/1]",   "[b, x/1, r/o/1]",   "File, File in recursive subdir, dir and recursive dir added, retained and removed in local"}
    });
  }

  @Test
  public void testGetDirDiff() throws IOException {
    // Setup
    Path localSnapshotDir = BlobStoreTestUtil.createLocalDir(this.local);
    String basePath = localSnapshotDir.toAbsolutePath().toString();
    DirIndex remoteSnapshotDir = BlobStoreTestUtil.createDirIndex(this.remote);

    // Execute
    DirDiff dirDiff = DirDiffUtil.getDirDiff(localSnapshotDir.toFile(), remoteSnapshotDir,
      (localFile, remoteFile) -> localFile.getName().equals(remoteFile.getFileName()));

    SortedSet<String> allAdded = new TreeSet<>();
    SortedSet<String> allRemoved = new TreeSet<>();
    SortedSet<String> allRetained = new TreeSet<>();
    BlobStoreTestUtil.getAllAddedInDiff(basePath, dirDiff, allAdded);
    BlobStoreTestUtil.getAllRemovedInDiff("", dirDiff, allRemoved);
    BlobStoreTestUtil.getAllRetainedInDiff("", dirDiff, allRetained);

    // Assert
    SortedSet<String> expectedAddedFiles = BlobStoreTestUtil.getExpected(this.expectedAdded);
    SortedSet<String> expectedRetainedFiles = BlobStoreTestUtil.getExpected(this.expectedRetained);
    SortedSet<String> expectedRemovedFiles = BlobStoreTestUtil.getExpected(this.expectedRemoved);
    assertEquals(expectedAddedFiles, allAdded);
    assertEquals(expectedRetainedFiles, allRetained);
    assertEquals(expectedRemovedFiles, allRemoved);
  }
}
