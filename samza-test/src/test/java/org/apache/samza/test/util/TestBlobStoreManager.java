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

package org.apache.samza.test.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import org.apache.commons.io.FileUtils;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.config.Config;
import org.apache.samza.storage.blobstore.BlobStoreManager;
import org.apache.samza.storage.blobstore.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBlobStoreManager implements BlobStoreManager {
  private static final Logger LOG = LoggerFactory.getLogger(TestBlobStoreManager.class);
  public static final String BLOB_STORE_BASE_DIR = "blob.store.base.dir";
  public static final String BLOB_STORE_LEDGER_DIR = "blob.store.ledger.dir";
  public static final String LEDGER_FILES_ADDED = "filesAdded";
  public static final String LEDGER_FILES_READ = "filesRead";
  public static final String LEDGER_FILES_DELETED = "filesRemoved";
  public static final String LEDGER_FILES_TTL_UPDATED = "filesTTLUpdated";

  private final Path stateLocation;
  private final File filesAddedLedger;
  private final File filesReadLedger;
  private final File filesDeletedLedger;
  private final File filesTTLUpdatedLedger;

  public TestBlobStoreManager(Config config, ExecutorService executorService) {
    this.stateLocation = Paths.get(config.get(BLOB_STORE_BASE_DIR));
    Path ledgerLocation = Paths.get(config.get(BLOB_STORE_LEDGER_DIR));
    try {
      if (Files.notExists(ledgerLocation)) {
        Files.createDirectories(ledgerLocation);
      }

      filesAddedLedger = Paths.get(ledgerLocation.toString(), LEDGER_FILES_ADDED).toFile();
      filesReadLedger = Paths.get(ledgerLocation.toString(), LEDGER_FILES_READ).toFile();
      filesDeletedLedger = Paths.get(ledgerLocation.toString(), LEDGER_FILES_DELETED).toFile();
      filesTTLUpdatedLedger = Paths.get(ledgerLocation.toString(), LEDGER_FILES_TTL_UPDATED).toFile();

      FileUtils.touch(filesAddedLedger);
      FileUtils.touch(filesReadLedger);
      FileUtils.touch(filesDeletedLedger);
      FileUtils.touch(filesTTLUpdatedLedger);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void init() {
  }

  @Override
  public CompletionStage<String> put(InputStream inputStream, Metadata metadata) {
    String payloadPath = metadata.getPayloadPath();
    String suffix;
    if (payloadPath.equals(Metadata.SNAPSHOT_INDEX_PAYLOAD_PATH)) {
      // include (fake checkpoint ID as an) unique ID in path for snapshot index blobs to avoid overwriting
      suffix = payloadPath + "-" + CheckpointId.create();
    } else {
      String[] parts = payloadPath.split("/");
      String checkpointId = parts[parts.length - 2];
      String fileName = parts[parts.length - 1];
      suffix = checkpointId + "/" + fileName;
    }

    Path destination = Paths.get(stateLocation.toString(), metadata.getJobName(), metadata.getJobId(),
        metadata.getTaskName(), metadata.getStoreName(), suffix);
    LOG.info("Creating file at {}", destination);
    try {
      FileUtils.writeStringToFile(filesAddedLedger, destination + "\n", Charset.defaultCharset(), true);
      FileUtils.copyInputStreamToFile(inputStream, destination.toFile());
    } catch (IOException e) {
      throw new RuntimeException("Error creating file " + destination, e);
    }
    return CompletableFuture.completedFuture(destination.toString());
  }

  @Override
  public CompletionStage<Void> get(String id, OutputStream outputStream, Metadata metadata) {
    LOG.info("Reading file at {}", id);
    try {
      FileUtils.writeStringToFile(filesReadLedger, id + "\n", Charset.defaultCharset(), true);
      Path path = Paths.get(id);
      Files.copy(path, outputStream);
      outputStream.flush();
    } catch (IOException e) {
      throw new RuntimeException("Error reading file for id " + id, e);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletionStage<Void> delete(String id, Metadata metadata) {
    LOG.info("Deleting file at {}", id);
    try {
      FileUtils.writeStringToFile(filesDeletedLedger, id + "\n", Charset.defaultCharset(), true);
      Files.delete(Paths.get(id));
    } catch (IOException e) {
      throw new RuntimeException("Error deleting file for id " + id, e);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletionStage<Void> removeTTL(String blobId, Metadata metadata) {
    LOG.info("Removing TTL (no-op) for file at {}", blobId);
    try {
      FileUtils.writeStringToFile(filesTTLUpdatedLedger, blobId + "\n", Charset.defaultCharset(), true);
    } catch (IOException e) {
      throw new RuntimeException("Error updating ttl for id " + blobId, e);
    }

    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void close() {
  }
}
