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
import java.util.List;
import org.apache.samza.storage.blobstore.index.FileBlob;
import org.apache.samza.storage.blobstore.index.FileMetadata;


/**
 * A mix-in Jackson class to convert FileIndex to/from JSON.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class JsonFileIndexMixin {
  @JsonCreator
  public JsonFileIndexMixin(@JsonProperty("file-name") String fileName,
      @JsonProperty("blobs") List<FileBlob> blobs, @JsonProperty("file-metadata") FileMetadata fileMetadata,
      @JsonProperty("checksum") long checksum) {

  }

  @JsonProperty("file-name")
  abstract String getFileName();

  @JsonProperty("blobs")
  abstract List<FileBlob> getBlobs();

  @JsonProperty("file-metadata")
  abstract FileMetadata getFileMetadata();

  @JsonProperty("checksum")
  abstract long getChecksum();
}
