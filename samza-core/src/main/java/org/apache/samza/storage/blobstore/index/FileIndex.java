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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


/**
 * Representation of a file in blob store
 */
public class FileIndex {
  private final String fileName;
  /**
   * Chunks of file uploaded to blob store as {@link FileBlob}s
   */
  private final List<FileBlob> fileBlobs;
  /**
   * Metadata (e.g. POSIX file attributes) associated with the file.
   */
  private final FileMetadata fileMetadata;
  /**
   * Checksum of the file for verifying integrity.
   */
  private final long checksum;


  public FileIndex(String fileName, List<FileBlob> fileBlobs, FileMetadata fileMetadata, long checksum) {
    Preconditions.checkState(StringUtils.isNotBlank(fileName));
    Preconditions.checkNotNull(fileBlobs);
    // fileBlobs can be empty list for a file of size 0 bytes.
    Preconditions.checkNotNull(fileMetadata);
    this.fileName = fileName;
    this.fileBlobs = fileBlobs;
    this.fileMetadata = fileMetadata;
    this.checksum = checksum;
  }

  public String getFileName() {
    return fileName;
  }

  public List<FileBlob> getBlobs() {
    return fileBlobs;
  }

  public FileMetadata getFileMetadata() {
    return fileMetadata;
  }

  public long getChecksum() {
    return checksum;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof FileIndex)) {
      return false;
    }

    FileIndex that = (FileIndex) o;

    return new EqualsBuilder()
        .append(getFileName(), that.getFileName())
        .append(getBlobs(), that.getBlobs())
        .append(fileMetadata, that.fileMetadata)
        .append(getChecksum(), that.getChecksum())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getFileName())
        .append(getBlobs())
        .append(fileMetadata)
        .append(getChecksum())
        .toHashCode();
  }

  @Override
  public String toString() {
    return "FileIndex{" +
        "fileName='" + fileName + '\'' +
        ", fileBlobs=" + fileBlobs +
        ", fileMetadata=" + fileMetadata +
        ", checksum='" + checksum + '\'' +
        '}';
  }
}