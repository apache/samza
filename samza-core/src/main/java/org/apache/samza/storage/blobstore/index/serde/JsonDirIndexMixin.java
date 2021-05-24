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
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.FileIndex;


/**
 * A mix-in Jackson class to convert {@link DirIndex} to/from JSON.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class JsonDirIndexMixin {

  @JsonCreator
  public JsonDirIndexMixin(@JsonProperty("dir-name") String dirName,
      @JsonProperty("files-present") List<FileIndex> filesPresent,
      @JsonProperty("files-removed") List<FileIndex> filesRemoved,
      @JsonProperty("sub-dirs-present") List<DirIndex> subDirsPresent,
      @JsonProperty("sub-dirs-removed") List<DirIndex> subDirsRemoved) {
  }

  @JsonProperty("dir-name")
  abstract String getDirName();

  @JsonProperty("files-present")
  abstract List<FileIndex> getFilesPresent();

  @JsonProperty("files-removed")
  abstract List<FileIndex> getFilesRemoved();

  @JsonProperty("sub-dirs-present")
  abstract List<DirIndex> getSubDirsPresent();

  @JsonProperty("sub-dirs-removed")
  abstract List<DirIndex> getSubDirsRemoved();
}
