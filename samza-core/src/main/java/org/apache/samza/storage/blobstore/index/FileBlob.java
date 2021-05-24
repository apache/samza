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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


/**
 * Representation of a File in a Blob store
 */
public class FileBlob {

  private final String blobId;
  /**
   * Offset of this blob in the file. A file can be uploaded multiple chunks, and can have
   * multiple blobs associated with it. Each blob then has its own ID and an offset in the file.
   */
  private final int offset;

  public FileBlob(String blobId, int offset) {
    Preconditions.checkState(StringUtils.isNotBlank(blobId));
    Preconditions.checkState(offset >= 0);
    this.blobId = blobId;
    this.offset = offset;
  }

  public String getBlobId() {
    return blobId;
  }

  public int getOffset() {
    return offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof FileBlob)) {
      return false;
    }

    FileBlob fileBlob = (FileBlob) o;

    return new EqualsBuilder()
        .append(blobId, fileBlob.blobId)
        .append(offset, fileBlob.offset)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(blobId)
        .append(offset)
        .toHashCode();
  }

  @Override
  public String toString() {
    return "FileBlob{" + "blobId='" + blobId + '\'' + ", offset=" + offset + '}';
  }
}
