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

package org.apache.samza.storage.blobstore;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletionStage;
import org.apache.samza.annotation.InterfaceStability;


/**
 * Provides interface for common blob store operations: GET, PUT and DELETE
 */
@InterfaceStability.Unstable
public interface BlobStoreManager {
  /**
   * Initialize underlying blob store client, if necessary.
   *
   */
  void init();

  /**
   * Non-blocking PUT call to remote blob store with supplied metadata
   * @param inputStream InputStream to read the file
   * @param metadata user supplied {@link Metadata} of the request
   * @return a future containing the blob ID of the uploaded blob if the upload is successful.
   */
  CompletionStage<String> put(InputStream inputStream, Metadata metadata);

  /**
   * Non-blocking GET call to remote blob store
   * @param id Blob ID of the blob to get
   * @param outputStream OutputStream to write the downloaded blob
   * @param metadata User supplied {@link Metadata} of the request
   * @param getDeletedBlob Flag to indicate if get should try to get a blob marked for deletion but not yet compacted
   * @return A future that completes when all the chunks are downloaded and written successfully to the OutputStream
   * @throws org.apache.samza.storage.blobstore.exceptions.DeletedException returned future should complete
   *         exceptionally with DeletedException on failure with the blob already deleted error.
   */
  CompletionStage<Void> get(String id, OutputStream outputStream, Metadata metadata, boolean getDeletedBlob);

  /**
   * Non-blocking call to mark a blob for deletion in the remote blob store
   * @param id Blob ID of the blob to delete
   * @param metadata User supplied {@link Metadata} of the request
   * @return A future that completes when the blob is successfully deleted from the blob store.
   * @throws org.apache.samza.storage.blobstore.exceptions.DeletedException returned future should complete
   *         exceptionally with DeletedException on failure with the blob already deleted error. This exception is
   *         caught and ignored by the caller of the delete method during initial cleanup and SnapshotIndex read.
   */
  CompletionStage<Void> delete(String id, Metadata metadata);

  /**
   * Non-blocking call to remove the Time-To-Live (TTL) for a blob and make it permanent.
   * @param blobId Blob ID of blob to remove TTL for.
   * @param metadata User supplied {@link Metadata} of the request
   * @return a future that completes when the TTL for the blob is removed.
   * @throws org.apache.samza.storage.blobstore.exceptions.DeletedException returned future should complete
   *         exceptionally with DeletedException on failure with the blob already deleted error.
   */
  CompletionStage<Void> removeTTL(String blobId, Metadata metadata);

  /**
   * Cleanly close resources like blob store client
   */
  void close();
}
