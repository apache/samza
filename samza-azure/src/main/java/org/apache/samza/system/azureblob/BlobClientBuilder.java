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
package org.apache.samza.system.azureblob;

import com.azure.storage.blob.BlobServiceAsyncClient;


/**
 * Create a BlobServiceAsyncClient. Implementation controls construction of underlying client.
 * Customers implementing their own System Producer need to ensure thread safety of following impl for generation of client.
 * If org.apache.samza.system.azureblob.producer.AzureBlobSystemProducer is used, it by defaults allows only one thread to create the client.
 * Please ensure any client implementation of this interface to be thread safe as well.
 * AzureBlobSystemProducer also ensures to safely close the client on call of stop(). Please ensure to close clients if using this interface
 * to create your own client.
 */
public interface BlobClientBuilder {
  /**
   * Create a client for uploading to Azure Blob Storage
   * @return New instance of {@link BlobServiceAsyncClient}
   */
  BlobServiceAsyncClient getBlobServiceAsyncClient();
}
