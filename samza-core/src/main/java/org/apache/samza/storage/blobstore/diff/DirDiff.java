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

package org.apache.samza.storage.blobstore.diff;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.FileIndex;

/**
 * Representation of the diff between a local directory and a remote directory contents.
 */
public class DirDiff {

  private final String dirName;

  /**
   * New files in this directory that needs to be uploaded to the blob store.
   */
  private final List<File> filesAdded;

  /**
   * Files that have already been uploaded to the blob store in a previous snapshot and haven't changed.
   */
  private final List<FileIndex> filesRetained;

  /**
   * Files that have already been uploaded to the blob store in a previous snapshot and need to be removed.
   */
  private final List<FileIndex> filesRemoved;

  /**
   * Subdirectories of this directory that are not already present in the previous snapshot and all of their contents
   * need to be recursively added.
   */
  private final List<DirDiff> subDirsAdded;

  /**
   * Subdirectories of this directory that are already present in the previous snapshot, but whose contents
   * may have changed and may need to be recursively added or removed.
   */
  private final List<DirDiff> subDirsRetained;

  /**
   * Subdirectories that are already present in the previous snapshot, but don't exist in the local snapshot,
   * and hence all of their contents need to be recursively removed.
   */
  private final List<DirIndex> subDirsRemoved;

  public DirDiff(String dirName,
      List<File> filesAdded, List<FileIndex> filesRetained, List<FileIndex> filesRemoved,
      List<DirDiff> subDirsAdded, List<DirDiff> subDirsRetained, List<DirIndex> subDirsRemoved) {
    Preconditions.checkNotNull(dirName); // may be empty for root dirs
    Preconditions.checkNotNull(filesAdded);
    Preconditions.checkNotNull(filesRetained);
    Preconditions.checkNotNull(filesRemoved);
    Preconditions.checkNotNull(subDirsAdded);
    Preconditions.checkNotNull(subDirsRetained);
    Preconditions.checkNotNull(subDirsRemoved);

    // validate that a file is not present in multiple lists
    Set<String> addedFilesSet = filesAdded.stream().map(File::getName).collect(Collectors.toSet());
    Set<String> retainedFilesSet = filesRetained.stream().map(FileIndex::getFileName).collect(Collectors.toSet());
    Set<String> removedFilesSet = filesRemoved.stream().map(FileIndex::getFileName).collect(Collectors.toSet());
    Sets.SetView<String> addedAndRetainedFilesSet = Sets.intersection(addedFilesSet, retainedFilesSet);
    Preconditions.checkState(addedAndRetainedFilesSet.isEmpty(),
        String.format("Files present in both added and retained sets: %s", addedAndRetainedFilesSet.toString()));
    Sets.SetView<String> retainedAndRemovedFilesSet = Sets.intersection(retainedFilesSet, removedFilesSet);
    Preconditions.checkState(retainedAndRemovedFilesSet.isEmpty(),
        String.format("Files present in both retained and removed sets: %s", retainedAndRemovedFilesSet.toString()));
    // mutable files *names* like CURRENT may be present in both added and removed sets,
    // which is fine since they need to be re-uploaded and the old blobs need to be deleted

    // validate that a subDir is not present in multiple lists
    Set<String> addedSubDirsSet = subDirsAdded.stream().map(DirDiff::getDirName).collect(Collectors.toSet());
    Set<String> retainedSubDirsSet = subDirsRetained.stream().map(DirDiff::getDirName).collect(Collectors.toSet());
    Set<String> removedSubDirsSet = subDirsRemoved.stream().map(DirIndex::getDirName).collect(Collectors.toSet());
    Sets.SetView<String> addedAndRetainedSubDirsSet = Sets.intersection(addedSubDirsSet, retainedSubDirsSet);
    Preconditions.checkState(addedAndRetainedSubDirsSet.isEmpty(),
        String.format("Sub-dirs present in both added and retained sets: %s", addedAndRetainedSubDirsSet.toString()));
    Sets.SetView<String> retainedAndRemovedSubDirsSet = Sets.intersection(retainedSubDirsSet, removedSubDirsSet);
    Preconditions.checkState(retainedAndRemovedSubDirsSet.isEmpty(),
        String.format("Sub-dirs present in both retained and removed sets: %s", retainedAndRemovedSubDirsSet.toString()));

    this.dirName = dirName;
    this.filesAdded = filesAdded;
    this.filesRetained = filesRetained;
    this.filesRemoved = filesRemoved;
    this.subDirsAdded = subDirsAdded;
    this.subDirsRetained = subDirsRetained;
    this.subDirsRemoved = subDirsRemoved;
  }

  public String getDirName() {
    return dirName;
  }

  public List<File> getFilesAdded() {
    return filesAdded;
  }

  public List<FileIndex> getFilesRetained() {
    return filesRetained;
  }

  public List<FileIndex> getFilesRemoved() {
    return filesRemoved;
  }

  public List<DirDiff> getSubDirsAdded() {
    return subDirsAdded;
  }

  public List<DirDiff> getSubDirsRetained() {
    return subDirsRetained;
  }

  public List<DirIndex> getSubDirsRemoved() {
    return subDirsRemoved;
  }

  public static Stats getStats(DirDiff dirDiff) {
    Stats stats = new Stats();
    updateStats(dirDiff, stats);
    return stats;
  }

  private static void updateStats(DirDiff dirDiff, Stats stats) {
    stats.filesAdded += dirDiff.getFilesAdded().size();
    stats.filesRetained += dirDiff.getFilesRetained().size();
    stats.filesRemoved += dirDiff.getFilesRemoved().size();

    stats.bytesAdded += dirDiff.getFilesAdded().stream().mapToLong(File::length).sum();
    stats.bytesRetained += dirDiff.getFilesRetained().stream().mapToLong(f -> f.getFileMetadata().getSize()).sum();
    stats.bytesRemoved += dirDiff.getFilesRemoved().stream().mapToLong(f -> f.getFileMetadata().getSize()).sum();

    for (DirDiff subDirAdded: dirDiff.getSubDirsAdded()) {
      stats.subDirsAdded += 1;
      updateStats(subDirAdded, stats);
    }
    for (DirDiff subDirRetained: dirDiff.getSubDirsRetained()) {
      stats.subDirsRetained += 1;
      updateStats(subDirRetained, stats);
    }
    for (DirIndex subDirRemoved: dirDiff.getSubDirsRemoved()) {
      stats.subDirsRemoved += 1;
      updateStatsForDirRemoved(subDirRemoved, stats);
    }
  }

  private static void updateStatsForDirRemoved(DirIndex dirIndex, Stats stats) {
    // every file and sub-dir present in a removed parent dir are to be removed as well
    // files and sub-dirs to be removed don't matter since they would have already been
    // cleaned up after the previous commit
    stats.filesRemoved += dirIndex.getFilesRemoved().size();
    stats.bytesRemoved += dirIndex.getFilesPresent().stream().mapToLong(f -> f.getFileMetadata().getSize()).sum();
    for (DirIndex subDirRemoved: dirIndex.getSubDirsPresent()) {
      stats.subDirsRemoved += 1;
      updateStatsForDirRemoved(subDirRemoved, stats);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    DirDiff dirDiff = (DirDiff) o;

    return new EqualsBuilder()
        .append(getDirName(), dirDiff.getDirName())
        .append(getFilesAdded(), dirDiff.getFilesAdded())
        .append(getFilesRetained(), dirDiff.getFilesRetained())
        .append(getFilesRemoved(), dirDiff.getFilesRemoved())
        .append(getSubDirsAdded(), dirDiff.getSubDirsAdded())
        .append(getSubDirsRetained(), dirDiff.getSubDirsRetained())
        .append(getSubDirsRemoved(), dirDiff.getSubDirsRemoved())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getDirName())
        .append(getFilesAdded())
        .append(getFilesRetained())
        .append(getFilesRemoved())
        .append(getSubDirsAdded())
        .append(getSubDirsRetained())
        .append(getSubDirsRemoved())
        .toHashCode();
  }

  public static class Stats {
    public int filesAdded;
    public int filesRetained;
    public int filesRemoved;

    public int subDirsAdded;
    public int subDirsRetained;
    public int subDirsRemoved;

    public long bytesAdded;
    public long bytesRetained;
    public long bytesRemoved;

    @Override
    public String toString() {
      return "Stats{" +
          "filesAdded=" + filesAdded +
          ", filesRetained=" + filesRetained +
          ", filesRemoved=" + filesRemoved +
          ", subDirsAdded=" + subDirsAdded +
          ", subDirsRetained=" + subDirsRetained +
          ", subDirsRemoved=" + subDirsRemoved +
          ", bytesAdded=" + bytesAdded +
          ", bytesRetained=" + bytesRetained +
          ", bytesRemoved=" + bytesRemoved +
          '}';
    }
  }
}
