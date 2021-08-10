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
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


/**
 * A {@link SnapshotIndex} contains all the information necessary for recreating the local store by
 * downloading its contents from the remote blob store. The {@link SnapshotIndex} is itself serialized
 * and stored as a blob in the remote store, and its blob id tracked in the Task checkpoint.
 */
public class SnapshotIndex {
  private static final short SCHEMA_VERSION = 1;

  private final long creationTimeMillis;
  /**
   * Metadata for a snapshot like job name, job Id, store name etc.
   */
  private final SnapshotMetadata snapshotMetadata;
  private final DirIndex dirIndex;

  /**
   * Blob ID of previous snapshot index blob. Tracked here to be cleaned up
   * in cleanup phase of commit lifecycle.
   */
  private final Optional<String> prevSnapshotIndexBlobId;

  public SnapshotIndex(long creationTimeMillis, SnapshotMetadata snapshotMetadata, DirIndex dirIndex,
      Optional<String> prevSnapshotIndexBlobId) {
    Preconditions.checkState(creationTimeMillis >= 0);
    Preconditions.checkNotNull(snapshotMetadata);
    Preconditions.checkNotNull(dirIndex);
    Preconditions.checkNotNull(prevSnapshotIndexBlobId);
    Preconditions.checkState(
        !(prevSnapshotIndexBlobId.isPresent() && StringUtils.isBlank(prevSnapshotIndexBlobId.get())));
    this.creationTimeMillis = creationTimeMillis;
    this.snapshotMetadata = snapshotMetadata;
    this.dirIndex = dirIndex;
    // if no previous snapshot index blob was present, this can be null
    this.prevSnapshotIndexBlobId = prevSnapshotIndexBlobId;
  }

  public static short getSchemaVersion() {
    return SCHEMA_VERSION;
  }

  public long getCreationTimeMillis() {
    return creationTimeMillis;
  }

  public SnapshotMetadata getSnapshotMetadata() {
    return snapshotMetadata;
  }

  public DirIndex getDirIndex() {
    return dirIndex;
  }

  public Optional<String> getPrevSnapshotIndexBlobId() {
    return prevSnapshotIndexBlobId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof SnapshotIndex)) {
      return false;
    }

    SnapshotIndex that = (SnapshotIndex) o;

    return new EqualsBuilder()
        .append(getCreationTimeMillis(), that.getCreationTimeMillis())
        .append(getSnapshotMetadata(), that.getSnapshotMetadata())
        .append(getDirIndex(), that.getDirIndex())
        .append(getPrevSnapshotIndexBlobId(), that.getPrevSnapshotIndexBlobId())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getCreationTimeMillis())
        .append(getSnapshotMetadata())
        .append(getDirIndex())
        .append(prevSnapshotIndexBlobId)
        .toHashCode();
  }

  @Override
  public String toString() {
    return "SnapshotIndex{" +
        "creationTimeMillis=" + creationTimeMillis +
        ", snapshotMetadata=" + snapshotMetadata +
        ", dirIndex=" + dirIndex +
        ", prevSnapshotIndexBlobId" + prevSnapshotIndexBlobId +
        '}';
  }
}