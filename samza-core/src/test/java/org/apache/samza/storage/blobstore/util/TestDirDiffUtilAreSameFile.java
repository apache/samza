package org.apache.samza.storage.blobstore.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.zip.CRC32;
import junit.framework.Assert;
import org.apache.samza.storage.blobstore.index.FileIndex;
import org.apache.samza.storage.blobstore.index.FileMetadata;
import org.junit.Before;
import org.junit.Test;

public class TestDirDiffUtilAreSameFile

{
  public static final int SMALL_FILE = 100;
  public static final int LARGE_FILE = 1024 * 1024 + 1;
  private BiPredicate<File, FileIndex> areSameFile = null;

  File localFile = null;

  FileIndex remoteFile = null;
  private long localChecksum = 0;
  private PosixFileAttributes localFileAttrs = null;
  private FileMetadata remoteFileMetadata = null;
  private long localContentLength = 0;

  @Before
  public void testSetup() throws Exception {
    areSameFile = DirDiffUtil.areSameFile(false);
    createFile(SMALL_FILE);
  }

  void createFile(int fileSize) throws Exception {
    localFile = File.createTempFile("temp", null);
    final String data = "a";
    CRC32 crc32 = new CRC32();
    try(FileWriter writer = new FileWriter(localFile);
        BufferedWriter bw = new BufferedWriter(writer)) {
      for(int i =0; i < fileSize; i++) {
        crc32.update(data.getBytes(StandardCharsets.UTF_8));
        bw.write(data);
      }
    }
    localContentLength = localFile.length();
    localChecksum = crc32.getValue();

    localFileAttrs = Files.readAttributes(localFile.toPath(), PosixFileAttributes.class);

    remoteFileMetadata = new FileMetadata(0,0,
        localContentLength,
        localFileAttrs.owner().getName(),
        localFileAttrs.group().getName(),
        PosixFilePermissions.toString(localFileAttrs.permissions()));

    remoteFile = new FileIndex(localFile.getName(), new ArrayList<>(), remoteFileMetadata, localChecksum);
    localFile.deleteOnExit();
  }

  @Test
  public void testAreSameFile_SameFile() {
    Assert.assertTrue(areSameFile.test(localFile, remoteFile));
  }

  @Test
  public void testAreSameFile_DifferentFile() {
    remoteFile = new FileIndex(localFile.getName() + "_other", new ArrayList<>(), remoteFileMetadata, localChecksum + 1);
    Assert.assertFalse(areSameFile.test(localFile, remoteFile));
  }

  @Test
  public void testAreSameFile_DifferentCrc() {
    remoteFile = new FileIndex(localFile.getName(), new ArrayList<>(), remoteFileMetadata, localChecksum + 1);
    Assert.assertFalse(areSameFile.test(localFile, remoteFile));
  }

  @Test
  public void testAreSameFile_DifferentSize() {
    remoteFileMetadata = new FileMetadata(0,0,
        localContentLength + 1,
        localFileAttrs.owner().getName(),
        localFileAttrs.group().getName(),
        PosixFilePermissions.toString(localFileAttrs.permissions()));
    remoteFile = new FileIndex(localFile.getName(), new ArrayList<>(), remoteFileMetadata, localChecksum);
    Assert.assertFalse(areSameFile.test(localFile, remoteFile));
  }

  @Test
  public void testAreSameFile_DifferentOwner() {
    remoteFileMetadata = new FileMetadata(0,0,
        localContentLength,
        localFileAttrs.owner().getName() + "_different",
        localFileAttrs.group().getName(),
        PosixFilePermissions.toString(localFileAttrs.permissions()));
    remoteFile = new FileIndex(localFile.getName(), new ArrayList<>(), remoteFileMetadata, localChecksum);
    Assert.assertFalse(areSameFile.test(localFile, remoteFile));
  }

  @Test
  public void testAreSameFile_DifferentGroup() {
    remoteFileMetadata = new FileMetadata(0,0,
        localContentLength,
        localFileAttrs.owner().getName(),
        localFileAttrs.group().getName() + "_different",
        PosixFilePermissions.toString(localFileAttrs.permissions()));
    remoteFile = new FileIndex(localFile.getName(), new ArrayList<>(), remoteFileMetadata, localChecksum);
    Assert.assertFalse(areSameFile.test(localFile, remoteFile));
  }

  @Test
  public void testAreSameFile_DifferentOwnerRead() {
    Set<PosixFilePermission> remoteFilePermissions = localFileAttrs.permissions();
    remoteFilePermissions.remove(PosixFilePermission.OWNER_READ);
    remoteFileMetadata = new FileMetadata(0,0,
        localContentLength,
        localFileAttrs.owner().getName(),
        localFileAttrs.group().getName(),
        PosixFilePermissions.toString(remoteFilePermissions));
    remoteFile = new FileIndex(localFile.getName(), new ArrayList<>(), remoteFileMetadata, localChecksum);
    Assert.assertFalse(areSameFile.test(localFile, remoteFile));
  }

  @Test
  public void testAreSameFile_DifferentOwnerWrite() {
    Set<PosixFilePermission> remoteFilePermissions = localFileAttrs.permissions();
    remoteFilePermissions.remove(PosixFilePermission.OWNER_WRITE);
    remoteFileMetadata = new FileMetadata(0,0,
        localContentLength,
        localFileAttrs.owner().getName(),
        localFileAttrs.group().getName(),
        PosixFilePermissions.toString(remoteFilePermissions));
    remoteFile = new FileIndex(localFile.getName(), new ArrayList<>(), remoteFileMetadata, localChecksum);
    Assert.assertFalse(areSameFile.test(localFile, remoteFile));
  }

  @Test
  public void testAreSameFile_DifferentOwnerExecute() {
    Set<PosixFilePermission> remoteFilePermissions = localFileAttrs.permissions();
    remoteFilePermissions.add(PosixFilePermission.OWNER_EXECUTE);
    remoteFileMetadata = new FileMetadata(0,0,
        localContentLength,
        localFileAttrs.owner().getName(),
        localFileAttrs.group().getName(),
        PosixFilePermissions.toString(remoteFilePermissions));
    remoteFile = new FileIndex(localFile.getName(), new ArrayList<>(), remoteFileMetadata, localChecksum);
    Assert.assertFalse(areSameFile.test(localFile, remoteFile));
  }

  @Test
  public void testAreSameFile_SmallFile_DifferentCrc() {
    remoteFile = new FileIndex(localFile.getName(), new ArrayList<>(), remoteFileMetadata, localChecksum + 1);
    Assert.assertFalse(areSameFile.test(localFile, remoteFile));
  }

  @Test
  public void testAreSameFile_LargeFile_DifferentCrc() throws Exception {
    createFile(LARGE_FILE);
    remoteFile = new FileIndex(localFile.getName(), new ArrayList<>(), remoteFileMetadata, localChecksum + 1);
    Assert.assertTrue(areSameFile.test(localFile, remoteFile));
  }
}
