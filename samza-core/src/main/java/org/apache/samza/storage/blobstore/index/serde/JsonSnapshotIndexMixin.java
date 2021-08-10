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

package org.apache.samza.storage.blobstore.index.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;


/**
 * A mix-in Jackson class to convert SnapshotIndex to/from JSON.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class JsonSnapshotIndexMixin {
  @JsonCreator
  public JsonSnapshotIndexMixin(@JsonProperty("creation-time-millis") long creationTimeMillis,
      @JsonProperty("snapshot-metadata") SnapshotMetadata snapshotMetadata,
      @JsonProperty("dir-index") DirIndex dirIndex,
      @JsonProperty("prev-snapshot-index-blob-id") Optional<String> prevSnapshotIndexBlobId) {
  }

  @JsonProperty("creation-time-millis")
  abstract long getCreationTimeMillis();

  @JsonProperty("snapshot-metadata")
  abstract SnapshotMetadata getSnapshotMetadata();

  @JsonProperty("dir-index")
  abstract DirIndex getDirIndex();

  @JsonProperty("prev-snapshot-index-blob-id")
  abstract Optional<String> getPrevSnapshotIndexBlobId();
}
