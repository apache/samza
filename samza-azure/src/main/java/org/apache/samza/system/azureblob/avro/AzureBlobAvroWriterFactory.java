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

package org.apache.samza.system.azureblob.avro;

import org.apache.samza.system.azureblob.compression.Compression;
import org.apache.samza.system.azureblob.producer.AzureBlobWriter;
import org.apache.samza.system.azureblob.producer.AzureBlobWriterFactory;
import org.apache.samza.system.azureblob.producer.AzureBlobWriterMetrics;
import com.azure.storage.blob.BlobContainerAsyncClient;
import java.io.IOException;
import java.util.concurrent.Executor;


public class AzureBlobAvroWriterFactory implements AzureBlobWriterFactory {

  /**
   * {@inheritDoc}
   */
  public AzureBlobWriter getWriterInstance(BlobContainerAsyncClient containerAsyncClient, String blobURL,
      Executor blobUploadThreadPool, AzureBlobWriterMetrics metrics,
      int maxBlockFlushThresholdSize, long flushTimeoutMs, Compression compression, boolean useRandomStringInBlobName,
      long maxBlobSize, long maxMessagesPerBlob) throws IOException {
    return new AzureBlobAvroWriter(containerAsyncClient, blobURL, blobUploadThreadPool, metrics,
        maxBlockFlushThresholdSize, flushTimeoutMs, compression, useRandomStringInBlobName, maxBlobSize, maxMessagesPerBlob);
  }
}
