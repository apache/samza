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

package org.apache.samza.system.azureblob.producer;

import org.apache.samza.system.azureblob.compression.Compression;
import com.azure.storage.blob.BlobContainerAsyncClient;
import java.io.IOException;
import java.util.concurrent.Executor;
import org.apache.samza.config.Config;
import org.apache.samza.system.azureblob.utils.BlobMetadataGeneratorFactory;


public interface AzureBlobWriterFactory {
  /**
   * creates an instance of AzureBlobWriter.
   * @param containerAsyncClient Azure container client
   * @param blobURL Azure blob url
   * @param blobUploadThreadPool thread pool to be used by writer for uploading
   * @param metrics metrics to measure the number of bytes written by writer
   * @param blobMetadataGeneratorFactory factory to get generator for metadata properties for a blob
   * @param streamName name of the stream that this AzureBlobWriter is associated with
   * @param maxBlockFlushThresholdSize threshold at which to upload
   * @param flushTimeoutMs timeout after which the flush is abandoned
   * @param initBufferSize initial size of in-memory buffer(s)
   * @return AzureBlobWriter instance
   * @throws IOException if writer creation fails
   */
  AzureBlobWriter getWriterInstance(BlobContainerAsyncClient containerAsyncClient, String blobURL,
      Executor blobUploadThreadPool, AzureBlobWriterMetrics metrics,
      BlobMetadataGeneratorFactory blobMetadataGeneratorFactory, Config blobMetadataGeneratorConfig, String streamName,
      int maxBlockFlushThresholdSize, long flushTimeoutMs, Compression compression, boolean useRandomStringInBlobName,
      long maxBlobSize, long maxMessagesPerBlob, int initBufferSize) throws IOException;
}
