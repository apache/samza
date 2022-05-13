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

package org.apache.samza.storage.blobstore.index;

import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

public class TestDirIndex {

  @Test
  public void testDirIndexValidationForFileAddedAndRemovedChecksBlobsAndMetadataToo() {
    FileBlob filePresentBlob = new FileBlob("filePresentBlobId", 0);
    FileMetadata filePresentMetadata = new FileMetadata(1234L, 2345L, 12345, "owner", "group", "permission");
    FileIndex filePresent = new FileIndex("mutableFile",
        Collections.singletonList(filePresentBlob), filePresentMetadata, 789L);

    // same name and metadata, different blobs
    FileBlob fileRemovedBlob = new FileBlob("fileRemovedBlobId", 0);
    FileIndex fileRemoved = new FileIndex("mutableFile",
        Collections.singletonList(fileRemovedBlob), filePresentMetadata, 789L);

    new DirIndex("dir", Collections.singletonList(filePresent), Collections.singletonList(fileRemoved),
        Collections.emptyList(), Collections.emptyList());

    // but if same name and blobs, different metadata, should fail due to blob duplication validation
    try {
      FileMetadata otherFileMetadata = new FileMetadata(541345L, 624L, 2125L, "owner", "group", "permission");
      FileIndex otherFileRemoved = new FileIndex("mutableFile",
          Collections.singletonList(filePresentBlob), otherFileMetadata, 789L);
      new DirIndex("dir", Collections.singletonList(filePresent), Collections.singletonList(otherFileRemoved),
          Collections.emptyList(), Collections.emptyList());
      Assert.fail("Should have failed validation if same blob present in file added and removed, even if same name");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testDirIndexValidationFailsIfSameBlobAddedAndRemoved() {
    FileBlob filePresentBlob = new FileBlob("filePresentBlobId", 0);
    FileMetadata filePresentMetadata = new FileMetadata(1234L, 2345L, 12345, "owner", "group", "permission");
    FileIndex filePresent = new FileIndex("filePresent",
        Collections.singletonList(filePresentBlob), filePresentMetadata, 789L);

    // different name and metadata, same blob id
    FileBlob fileRemovedBlob = new FileBlob("filePresentBlobId", 0);
    FileMetadata fileRemovedMetadata = new FileMetadata(324L, 625L, 4253L, "owner", "group", "permission");
    FileIndex fileRemoved = new FileIndex("fileRemoved",
        Collections.singletonList(fileRemovedBlob), fileRemovedMetadata, 1234L);

    // should fail since same blob id is present in file present and removed
    new DirIndex("dir", Collections.singletonList(filePresent), Collections.singletonList(fileRemoved),
        Collections.emptyList(), Collections.emptyList());
  }
}
