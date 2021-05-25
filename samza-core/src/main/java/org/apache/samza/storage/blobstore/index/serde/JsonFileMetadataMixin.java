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

/**
 * A mix-in Jackson class to convert FileMetadata to/from JSON.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class JsonFileMetadataMixin {
  @JsonCreator
  public JsonFileMetadataMixin(@JsonProperty("creation-time-millis") long creationTimeMillis,
      @JsonProperty("last-modified-time-millis") long lastModifiedTimeMillis, @JsonProperty("size") long size,
      @JsonProperty("owner") String owner, @JsonProperty("group") String group,
      @JsonProperty("permissions") String permissions) {
  }

  @JsonProperty("creation-time-millis")
  abstract long getCreationTimeMillis();

  @JsonProperty("last-modified-time-millis")
  abstract long getLastModifiedTimeMillis();

  @JsonProperty("size")
  abstract long getSize();

  @JsonProperty("owner")
  abstract String getOwner();

  @JsonProperty("group")
  abstract String getGroup();

  @JsonProperty("permissions")
  abstract String getPermissions();
}
