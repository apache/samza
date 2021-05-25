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

import com.google.common.collect.ImmutableList;
import org.apache.samza.storage.blobstore.diff.DirDiff;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.FileBlob;
import org.apache.samza.storage.blobstore.index.FileIndex;
import org.apache.samza.storage.blobstore.index.FileMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.util.FileUtil;

/**
 * Test util methods to work with local dirs, {@link DirDiff}s and {@link DirIndex}es.
 */
public class BlobStoreTestUtil {
  public static final String TEMP_DIR_PREFIX = "samza-blob-store-test-";

  public static Path createLocalDir(String files) throws IOException {
    Path tempDirPath = Files.createTempDirectory(TEMP_DIR_PREFIX);
    File tempDirFile = tempDirPath.toFile();
    String tempDirPathString = tempDirPath.toAbsolutePath().toString();

    if (files.length() == 2) return tempDirPath;
    String[] paths = files.substring(1, files.length() - 1).split(",");

    for (String path: paths) {
      path = path.trim();
      if (!path.contains("/")) {
        Path filePath = Files.createFile(Paths.get(tempDirPathString, path));
        new FileUtil().writeToTextFile(filePath.toFile(), path, false); // file contents == file name
        filePath.toFile().deleteOnExit();
      } else {
        String dirs = path.substring(0, path.lastIndexOf("/"));
        String file = path.substring(path.lastIndexOf("/") + 1);
        Path directories = Files.createDirectories(Paths.get(tempDirPathString, dirs));
        if (!StringUtils.isBlank(file)) { // can be blank for empty directories
          Path filePath = Paths.get(directories.toAbsolutePath().toString(), file);
          Files.createFile(filePath);
          new FileUtil().writeToTextFile(filePath.toFile(), file, false); // file contents == file name
          filePath.toFile().deleteOnExit();
        }
      }
    }

    deleteDirRecursivelyOnExit(tempDirFile);
    return tempDirPath;
  }

  public static DirIndex createDirIndex(String files) throws IOException {
    if (files.equals("[]")) { // empty dir
      return new DirIndex(DirIndex.ROOT_DIR_NAME, Collections.emptyList(), Collections.emptyList(),
          Collections.emptyList(), Collections.emptyList());
    }

    String[] paths = files.substring(1, files.length() - 1).split(",");
    Arrays.sort(paths);
    // actually create the directory structure in a temp dir so that file properties and checksums can be computed
    Path localDir = createLocalDir(files);
    DirTreeNode dirTree = createDirTree(localDir.toAbsolutePath().toString(), paths);
    return createDirIndex(localDir.toAbsolutePath().toString(), dirTree);
  }

  public static void getAllAddedInDiff(String basePath, DirDiff dirDiff, Set<String> allAdded) {
    for (File fileAdded: dirDiff.getFilesAdded()) {
      allAdded.add(fileAdded.getAbsolutePath().substring(basePath.length() + 1));
    }

    for (DirDiff dirAdded: dirDiff.getSubDirsAdded()) {
      getAllAddedInDiff(basePath, dirAdded, allAdded);
    }

    for (DirDiff dirRetained: dirDiff.getSubDirsRetained()) {
      getAllAddedInDiff(basePath, dirRetained, allAdded);
    }
  }

  public static void getAllRemovedInDiff(String basePath, DirDiff dirDiff, Set<String> allRemoved) {
    String prefix = basePath.isEmpty() ? basePath : basePath + "/";
    for (FileIndex fileRemoved: dirDiff.getFilesRemoved()) {
      allRemoved.add(prefix + fileRemoved.getFileName());
    }

    for (DirIndex dirRemoved: dirDiff.getSubDirsRemoved()) {
      getAllRemovedInRemovedSubDir(prefix + dirRemoved.getDirName(), dirRemoved, allRemoved);
    }

    for (DirDiff dirRetained: dirDiff.getSubDirsRetained()) {
      getAllRemovedInRetainedSubDir(prefix + dirRetained.getDirName(), dirRetained, allRemoved);
    }
  }

  public static void getAllRetainedInDiff(String basePath, DirDiff dirDiff, Set<String> allRetained) {
    String prefix = basePath.isEmpty() ? basePath : basePath + "/";
    for (FileIndex fileRetained: dirDiff.getFilesRetained()) {
      allRetained.add(prefix + fileRetained.getFileName());
    }

    for (DirDiff dirRetained: dirDiff.getSubDirsRetained()) {
      getAllRetainedInDiff(prefix + dirRetained.getDirName(), dirRetained, allRetained);
    }
  }

  public static void getAllPresentInIndex(String basePath, DirIndex dirIndex, Set<String> allPresent) {
    String prefix = basePath.isEmpty() ? basePath : basePath + "/";

    for (FileIndex filePresent: dirIndex.getFilesPresent()) {
      allPresent.add(prefix + filePresent.getFileName());
    }

    for (DirIndex dirPresent: dirIndex.getSubDirsPresent()) {
      getAllPresentInIndex(prefix + dirPresent.getDirName(), dirPresent, allPresent);
    }
  }

  public static void getAllRemovedInIndex(String basePath, DirIndex dirIndex, Set<String> allRemoved) {
    String prefix = basePath.isEmpty() ? basePath : basePath + "/";

    for (FileIndex fileRemoved: dirIndex.getFilesRemoved()) {
      allRemoved.add(prefix + fileRemoved.getFileName());
    }

    for (DirIndex dirRemoved: dirIndex.getSubDirsRemoved()) {
      getAllRemovedInRemovedSubDir(prefix + dirRemoved.getDirName(), dirRemoved, allRemoved);
    }

    for (DirIndex dirPresent: dirIndex.getSubDirsPresent()) {
      getAllRemovedInIndex(prefix + dirPresent.getDirName(), dirPresent, allRemoved);
    }
  }

  public static SortedSet<String> getExpected(String expectedFiles) {
    if (expectedFiles.length() == 2) return new TreeSet<>();
    String[] paths = expectedFiles.substring(1, expectedFiles.length() - 1).split(",");
    SortedSet<String> result = new TreeSet<>();
    for (String path: paths) {
      result.add(path.trim());
    }
    return result;
  }

  private static DirIndex createDirIndex(String baseDir, DirTreeNode root) {
    String dirName = root.fileName;
    List<FileIndex> filesPresent = new ArrayList<>();
    List<DirIndex> subDirsPresent = new ArrayList<>();

    List<FileIndex> filesRemoved = Collections.emptyList();
    List<DirIndex> subDirsRemoved = Collections.emptyList();

    for (DirTreeNode child: root.children) {
      if (!child.children.isEmpty()) {
        subDirsPresent.add(createDirIndex(baseDir + "/" + child.fileName, child));
      } else {
        filesPresent.add(createFileIndex(baseDir + "/" + child.fileName, child));
      }
    }

    return new DirIndex(dirName, filesPresent, filesRemoved, subDirsPresent, subDirsRemoved);
  }

  private static DirTreeNode createDirTree(String baseDir, String[] paths) {
    DirTreeNode root = new DirTreeNode();
    root.fileName = DirIndex.ROOT_DIR_NAME;

    for (String path: paths) {
      DirTreeNode pathRoot = root;
      path = path.trim();
      String[] pathParts = path.split("/");
      for (String pathPart: pathParts) {
        DirTreeNode childNode;

        Optional<DirTreeNode> childNodeOptional = pathRoot.children.stream()
            .filter(dtn -> dtn.fileName.equals(pathPart)).findFirst();

        if (childNodeOptional.isPresent()) {
          childNode = childNodeOptional.get();
        } else {
          childNode = new DirTreeNode();
          childNode.fileName = pathPart;
          pathRoot.children.add(childNode);
        }
        pathRoot = childNode;
      }
    }

    return root;
  }

  public static File createTestCheckpointDirectory(String storePath, String checkpointId) throws IOException {
    File checkpointDir = new File(storePath + "-" + checkpointId);
    FileUtils.copyDirectory(new File(storePath), checkpointDir);
    return checkpointDir;
  }

  private static FileIndex createFileIndex(String filePath, DirTreeNode node) {
    long checksum;
    FileMetadata fileMetadata;
    try {
      Path path = Paths.get(filePath);
      Checksum crc32 = new CRC32();
      byte[] fileBytes = Files.readAllBytes(path);
      crc32.update(fileBytes, 0, fileBytes.length);
      checksum = crc32.getValue();
      fileMetadata = FileMetadata.fromFile(path.toFile());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new FileIndex(node.fileName, ImmutableList.of(new FileBlob(node.fileName, 0)), fileMetadata, checksum);
  }

  // recursively adds all present files to allRemoved in removed subdirs
  private static void getAllRemovedInRemovedSubDir(String basePath, DirIndex dirIndex, Set<String> allRemoved) {
    String prefix = basePath.isEmpty() ? basePath : basePath + "/";
    for (FileIndex f: dirIndex.getFilesPresent()) {
      allRemoved.add(prefix + f.getFileName());
    }

    for (DirIndex d: dirIndex.getSubDirsPresent()) {
      getAllRemovedInRemovedSubDir(prefix + d.getDirName(), d, allRemoved);
    }
  }

  // only adds removed files in retained subdirs
  private static void getAllRemovedInRetainedSubDir(String basePath, DirDiff dirDiff, Set<String> allRemoved) {
    String prefix = basePath.isEmpty() ? basePath : basePath + "/";
    for (FileIndex f: dirDiff.getFilesRemoved()) {
      allRemoved.add(prefix + f.getFileName());
    }

    for (DirIndex dirRemoved: dirDiff.getSubDirsRemoved()) {
      getAllRemovedInRemovedSubDir(prefix + dirRemoved.getDirName(), dirRemoved, allRemoved);
    }

    for (DirDiff dirRetained: dirDiff.getSubDirsRetained()) {
      getAllRemovedInRetainedSubDir(prefix + dirRetained.getDirName(), dirRetained, allRemoved);
    }
  }

  private static class DirTreeNode {
    String fileName;
    Set<DirTreeNode> children = new HashSet<>();
  }

  private static class MockFileMetadata extends FileMetadata {
    public MockFileMetadata() {
      super(0, 0, 0, "owner", "group", "rwxrw-r--");
    }
  }

  private static void deleteDirRecursivelyOnExit(File dir) {
    dir.deleteOnExit();
    for (File f: dir.listFiles()) {
      if (f.isDirectory()) {
        deleteDirRecursivelyOnExit(f);
      } else {
        f.deleteOnExit();
      }
    }
  }
}
