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

package org.apache.samza.storage.blobstore.util;

import com.google.common.base.Preconditions;
import org.apache.samza.storage.blobstore.diff.DirDiff;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.FileIndex;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides helper methods to create a {@link DirDiff} between local and remote snapshots.
 */
public class DirDiffUtil {
  private static final Logger LOG = LoggerFactory.getLogger(DirDiffUtil.class);

  /**
   * Compare the local snapshot directory and the remote snapshot directory and return the recursive diff of the two as
   * a {@link DirDiff}.
   * @param localSnapshotDir File representing local snapshot root directory
   * @param remoteSnapshotDir {@link DirIndex} representing the remote snapshot directory
   * @param areSameFile A BiPredicate to test if a local and remote file are the same file
   * @return {@link DirDiff} representing the recursive diff of local and remote snapshots directories
   */
  public static DirDiff getDirDiff(File localSnapshotDir, DirIndex remoteSnapshotDir,
      BiPredicate<File, FileIndex> areSameFile) {
    return getDirDiff(localSnapshotDir, remoteSnapshotDir, areSameFile, true);
  }

  private static DirDiff getDirDiff(File localSnapshotDir, DirIndex remoteSnapshotDir,
      BiPredicate<File, FileIndex> areSameFile, boolean isRootDir) {
    Preconditions.checkState(localSnapshotDir != null && localSnapshotDir.isDirectory());
    Preconditions.checkNotNull(remoteSnapshotDir);

    LOG.debug("Creating DirDiff between local dir: {} and remote dir: {}",
        localSnapshotDir.getPath(), remoteSnapshotDir.getDirName());
    List<DirDiff> subDirsAdded = new ArrayList<>();
    List<DirDiff> subDirsRetained = new ArrayList<>();
    List<DirIndex> subDirsRemoved = new ArrayList<>();

    // list files returns empty list if local snapshot directory is empty
    List<File> localSnapshotFiles = Arrays.asList(Objects.requireNonNull(localSnapshotDir.listFiles(File::isFile)));
    List<FileIndex> remoteSnapshotFiles = remoteSnapshotDir.getFilesPresent();

    // list files returns empty list if local snapshot directory is empty
    List<File> localSnapshotSubDirs = Arrays.asList(Objects.requireNonNull(localSnapshotDir.listFiles(File::isDirectory)));
    Set<String> localSnapshotSubDirNames = localSnapshotSubDirs.stream()
        .map(File::getName)
        .collect(Collectors.toCollection(HashSet::new));

    List<DirIndex> remoteSnapshotSubDirs = remoteSnapshotDir.getSubDirsPresent();
    Set<String> remoteSnapshotSubDirNames = remoteSnapshotSubDirs.stream()
        .map(DirIndex::getDirName)
        .collect(Collectors.toCollection(HashSet::new));

    // TODO MED shesharm: this compares each file in directory 3 times. Categorize files in one traversal instead.
    List<File> filesToUpload = getNewFilesToUpload(remoteSnapshotFiles, localSnapshotFiles, areSameFile);
    List<FileIndex> filesToRetain = getFilesToRetain(remoteSnapshotFiles, localSnapshotFiles, areSameFile);
    List<FileIndex> filesToRemove = getFilesToRemove(remoteSnapshotFiles, localSnapshotFiles, areSameFile);

    for (File localSnapshotSubDir: localSnapshotSubDirs) {
      if (!remoteSnapshotSubDirNames.contains(localSnapshotSubDir.getName())) {
        LOG.debug("Subdir {} present in local snapshot but not in remote snapshot. " +
            "Recursively adding subdir contents.", localSnapshotSubDir.getPath());
        subDirsAdded.add(getDiffForNewDir(localSnapshotSubDir));
      } else {
        LOG.debug("Subdir {} present in local snapshot and in remote snapshot. " +
            "Recursively comparing local and remote subdirs.", localSnapshotSubDir.getPath());
        DirIndex remoteSubDirIndex =
            remoteSnapshotSubDirs.stream()
                .filter(indexBlob -> indexBlob.getDirName().equals(localSnapshotSubDir.getName()))
                .findFirst().get();
        subDirsRetained.add(getDirDiff(localSnapshotSubDir, remoteSubDirIndex, areSameFile, false));
      }
    }

    // 3. Subdir in remote snapshot but not in local snapshot
    for (DirIndex remoteSnapshotSubDir: remoteSnapshotSubDirs) {
      if (!localSnapshotSubDirNames.contains(remoteSnapshotSubDir.getDirName())) {
        LOG.debug("Subdir {} present in remote snapshot but not in local snapshot. " +
            "Marking for removal from remote snapshot. ", remoteSnapshotDir.getDirName());
        subDirsRemoved.add(remoteSnapshotSubDir);
      }
    }

    String dirName = isRootDir ? DirIndex.ROOT_DIR_NAME : localSnapshotDir.getName();
    return new DirDiff(dirName,
        filesToUpload, filesToRetain, filesToRemove,
        subDirsAdded, subDirsRetained, subDirsRemoved);
  }

  /**
   * Builds a {@link DirDiff} from a new local directory that is not already present in the remote snapshot.
   * @param localSubDir File representing the local directory to create the new {@link DirDiff} for.
   */
  private static DirDiff getDiffForNewDir(File localSubDir) {
    List<File> filesAdded = new ArrayList<>();
    List<DirDiff> subDirsAdded = new ArrayList<>();

    File[] files = localSubDir.listFiles();
    if (files != null) {
      for (File file: files) {
        if (file.isFile()) {
          LOG.debug("Adding file {} to local sub dir {}", file.getName(), localSubDir.getPath());
          filesAdded.add(file);
        } else {
          LOG.debug("Adding sub dir {} to sub dir {}", file.getName(), localSubDir.getPath());
          subDirsAdded.add(getDiffForNewDir(file));
        }
      }
    }

    return new DirDiff(localSubDir.getName(), filesAdded, Collections.emptyList(), Collections.emptyList(),
        subDirsAdded, Collections.emptyList(), Collections.emptyList());
  }

  /**
   * Returns a list of files uploaded in remote checkpoint that are not present in new local snapshot and needs to be
   * deleted/reclaimed from remote store.
   */
  private static List<FileIndex> getFilesToRemove(
      List<FileIndex> remoteSnapshotFiles, List<File> localSnapshotFiles,
      BiPredicate<File, FileIndex> areSameFile) {
    List<FileIndex> filesToRemove = new ArrayList<>();

    Map<String, File> localFiles = localSnapshotFiles.stream()
        .collect(Collectors.toMap(File::getName, Function.identity()));

    for (FileIndex remoteFile : remoteSnapshotFiles) {
      String remoteFileName = remoteFile.getFileName();
      if (!localFiles.containsKey(remoteFileName) ||
          !areSameFile.test(localFiles.get(remoteFileName), remoteFile)) {
        LOG.debug("File {} only present in remote snapshot or is not the same as local file.", remoteFile.getFileName());
        filesToRemove.add(remoteFile);
      }
    }

    return filesToRemove;
  }

  /**
   * Returns a list of files to be uploaded to remote store that are part of new snapshot created locally.
   */
  private static List<File> getNewFilesToUpload(
      List<FileIndex> remoteSnapshotFiles, List<File> localSnapshotFiles,
      BiPredicate<File, FileIndex> areSameFile) {
    List<File> filesToUpload = new ArrayList<>();

    Map<String, FileIndex> remoteFiles = remoteSnapshotFiles.stream()
        .collect(Collectors.toMap(FileIndex::getFileName, Function.identity()));

    for (File localFile: localSnapshotFiles) {
      String localFileName = localFile.getName();
      if (!remoteFiles.containsKey(localFileName) ||
          !areSameFile.test(localFile, remoteFiles.get(localFileName))) {
        LOG.debug("File {} only present in local snapshot or is not the same as remote file.", localFile.getPath());
        filesToUpload.add(localFile);
      }
    }

    return filesToUpload;
  }

  /**
   * Returns a list of common files between local and remote snapshot. These files are reused from prev remote snapshot
   * and do not need to be uploaded again.
   */
  private static List<FileIndex> getFilesToRetain(
      List<FileIndex> remoteSnapshotFiles, List<File> localSnapshotFiles,
      BiPredicate<File, FileIndex> areSameFile) {
    List<FileIndex> filesToRetain = new ArrayList<>();

    Map<String, File> localFiles = localSnapshotFiles.stream()
        .collect(Collectors.toMap(File::getName, Function.identity()));

    for (FileIndex remoteFile : remoteSnapshotFiles) {
      String remoteFileName = remoteFile.getFileName();
      if (localFiles.containsKey(remoteFileName) &&
          areSameFile.test(localFiles.get(remoteFileName), remoteFile)) {
        String localFilePath = localFiles.get(remoteFileName).getPath();
        LOG.debug("File {} present in both local and remote snapshot and is the same.", localFilePath);
        filesToRetain.add(remoteFile);
      }
    }

    return filesToRetain;
  }
}