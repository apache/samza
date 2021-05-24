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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


/**
 * Representation of metadata associated with a File.
 */
public class FileMetadata {

  private final long creationTimeMillis;
  private final long lastModifiedTimeMillis;
  private final long size;
  private final String owner;
  private final String group;
  private final String permissions;

  public FileMetadata(long creationTimeMillis, long lastModifiedTimeMillis, long size,
      String owner, String group, String permissions) {
    Preconditions.checkState(creationTimeMillis >= 0);
    Preconditions.checkState(lastModifiedTimeMillis >= 0);
    Preconditions.checkState(size >= 0);
    Preconditions.checkState(StringUtils.isNotBlank(owner));
    Preconditions.checkState(StringUtils.isNotBlank(group));
    Preconditions.checkState(StringUtils.isNotBlank(permissions));
    this.creationTimeMillis = creationTimeMillis;
    this.lastModifiedTimeMillis = lastModifiedTimeMillis;
    this.size = size;
    this.owner = owner;
    this.group = group;
    this.permissions = permissions;
  }

  public static FileMetadata fromFile(File file) throws IOException {
    PosixFileAttributes attributes = Files.readAttributes(file.toPath(), PosixFileAttributes.class);

    return new FileMetadata(attributes.creationTime().toMillis(), attributes.lastModifiedTime().toMillis(),
        attributes.size(), attributes.owner().toString(), attributes.group().toString(),
        PosixFilePermissions.toString(attributes.permissions()));
  }

  public long getCreationTimeMillis() {
    return creationTimeMillis;
  }

  public long getLastModifiedTimeMillis() {
    return lastModifiedTimeMillis;
  }

  public long getSize() {
    return size;
  }

  public String getOwner() {
    return owner;
  }

  public String getGroup() {
    return group;
  }

  public String getPermissions() {
    return permissions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof FileMetadata)) {
      return false;
    }

    FileMetadata that = (FileMetadata) o;

    return new EqualsBuilder()
        .append(getCreationTimeMillis(), that.getCreationTimeMillis())
        .append(getLastModifiedTimeMillis(), that.getLastModifiedTimeMillis())
        .append(getSize(), that.getSize())
        .append(getOwner(), that.getOwner())
        .append(getGroup(), that.getGroup())
        .append(getPermissions(), that.getPermissions())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getCreationTimeMillis())
        .append(getLastModifiedTimeMillis())
        .append(getSize())
        .append(getOwner())
        .append(getGroup())
        .append(getPermissions())
        .toHashCode();
  }

  @Override
  public String toString() {
    return "FileMetadata{" +
        "creationTimeMillis=" + creationTimeMillis +
        ", lastModifiedTimeMillis=" + lastModifiedTimeMillis +
        ", size=" + size +
        ", owner='" + owner + '\'' +
        ", group='" + group + '\'' +
        ", permissions=" + permissions +
        '}';
  }
}
