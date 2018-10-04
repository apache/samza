package org.apache.samza.storage.kv;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.samza.SamzaException;
import org.rocksdb.RocksDB;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RocksDbBackupStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbBackupStorage.class);
  private FileSystem fs;
  private Path hdfsPath;
  private Path localPath;
  public RocksDbBackupStorage(String coreSitePath, String hdfsSitePath, String localPathStr, String hdfsPathStr) {
    localPath = new Path(localPathStr);
    hdfsPath = new Path(hdfsPathStr);

    Configuration conf = new Configuration();
    Path hdfsCoreSitePath = new Path(coreSitePath);
    Path hdfsHDFSSitePath = new Path(hdfsSitePath);
    conf.addResource(hdfsCoreSitePath);
    conf.addResource(hdfsHDFSSitePath);
    try {
      fs = FileSystem.get(conf);
      // No need to check if localPath is dir since it's checked when rocksdb is created
      if (fs.exists(hdfsPath)) {
        if(fs.isFile(hdfsPath)) {
          throw new SamzaException("Error creating backup in hdfs: Path is already a file");
        }
      } else {
        fs.mkdirs(hdfsPath);
      }

    } catch(IOException e) {
      throw new SamzaException("Failed to create hdfs filesystem from: " + e);
    }

  }

  // Should this be void?
  public Boolean createBackup() throws IOException {
    // TODO add flag to skip statistic tracking
    long startTime = System.currentTimeMillis();
    long totalSizeCopied = 0;
    // Get all files that are unique to local / modified since last backup (like manifest) and copy them to backup hdfs
    ArrayList<File> dbFilesToCopy = getExtraFilesInLocalCompareToBackup();
    for (File dbFile : dbFilesToCopy) {
      System.out.print(dbFilesToCopy);
      totalSizeCopied += dbFile.length();
      Path pathToFile = new Path(dbFile.getAbsolutePath());
      fs.copyFromLocalFile(false,true, pathToFile, hdfsPath);
    }

    // Print statistics of creating a backup
    long endTime = System.currentTimeMillis();
    long timeElapsed = endTime - startTime;
    LOGGER.info("Creating backup finished in milliseconds: " + timeElapsed);
    LOGGER.info("Copied " + dbFilesToCopy.size() + " files to backup");
    LOGGER.info("Total size copied over: " + totalSizeCopied);
    return true;
  }

  public Boolean restoreBackup(RocksDB db) throws IOException {
    // Test if doesn't delete srcDir files - doesn't
    fs.copyToLocalFile(hdfsPath, localPath);
    return true;
  }

  // Should be used when checking backup for any unnecessary files
  // Not 100% sure when this should run, after creating a backup?
  public ArrayList getExtraFilesInBackupCompareToLocal() throws IOException {
    HashMap<String, File> localFileHmap = new HashMap<String, File>();
    ArrayList<FileStatus> filesNotInLocalList = new ArrayList<FileStatus>();
    // Store all the Rocksdb files in hashmap for quick comparison
    File dbDir = new File(localPath.toString());
    File[] filesList = dbDir.listFiles();
    for (File dbFile : filesList) {
      String fileName = dbFile.getName();
      // Skip lock file
      if (fileName.equals("LOCK")) continue;
      localFileHmap.put(fileName, dbFile);
    }

    // Get all files in the hdfs backup and compare to local. Add if different/doesn't exist
    FileStatus[] backupFileIterator = fs.listStatus(hdfsPath);
    for (FileStatus backupFile : backupFileIterator) {
      String fileName = backupFile.getPath().getName();
      // Skip lock file
      if (fileName.equals("LOCK")) continue;

      if (localFileHmap.containsKey(fileName)) {
        // TODO: This may not be good enough check if things fail to copy or weird things happen, probably an edge case here to consider
        if (backupFile.getLen() < localFileHmap.get(fileName).length()) {
          filesNotInLocalList.add(backupFile);
        }
      } else {
        filesNotInLocalList.add(backupFile);
      }
    }

    return filesNotInLocalList;
  }

  public ArrayList<File> getExtraFilesInLocalCompareToBackup() throws IOException {
    HashMap<String, FileStatus> hdfsFileHmap = new HashMap<String, FileStatus>();
    ArrayList<File> filesNotInHDFSList = new ArrayList<File>();
    // Store all the backup files in hashmap for quick comparison
    FileStatus[] backupFileIterator = fs.listStatus(hdfsPath);
    for (FileStatus file : backupFileIterator) {
      String fileName = file.getPath().getName();
      // Skip lock file
      if (fileName.equals("LOCK"))
        continue;
      hdfsFileHmap.put(fileName, file);
    }

    // Get all files in the local db and compare to backup. Add if different/doesn't exist
    File dbDir = new File(localPath.toString());
    File[] filesList = dbDir.listFiles();
    for (File dbFile : filesList) {
      String fileName = dbFile.getName();
      // Skip lock file
      if (fileName.equals("LOCK"))
        continue;

      if (hdfsFileHmap.containsKey(fileName)) {
        // TODO: This may not be good enough check if things fail to copy or weird things happen, probably an edge case here to consider
        // Like if the files just happen to have the same length but different contents or smth because of overwritten?
        if (dbFile.length() > hdfsFileHmap.get(fileName).getLen()) {
          filesNotInHDFSList.add(dbFile);
        }
      } else {
        filesNotInHDFSList.add(dbFile);
      }
    }

    return filesNotInHDFSList;
  }

  public void closeFileSystem() throws IOException {
    fs.close();
  }

}
