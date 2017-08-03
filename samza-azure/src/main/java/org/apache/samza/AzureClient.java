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

package org.apache.samza;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.table.CloudTableClient;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Creates the client handles for the Azure Storage account, Azure Blob storage and Azure Table storage
 */
public class AzureClient {

  private static final Logger LOG = LoggerFactory.getLogger(AzureClient.class);
  private final CloudStorageAccount account;
  private final CloudTableClient tableClient;
  private final CloudBlobClient blobClient;

  AzureClient(String storageConnectionString) {
    try {
      account = CloudStorageAccount.parse(storageConnectionString);
      blobClient = account.createCloudBlobClient();
      tableClient = account.createCloudTableClient();
    } catch (IllegalArgumentException | URISyntaxException e) {
      LOG.error("\nConnection string {} specifies an invalid URI.", storageConnectionString);
      LOG.error("Please confirm the connection string is in the Azure connection string format.");
      throw new SamzaException(e);
    } catch (InvalidKeyException e) {
      LOG.error("\nConnection string {} specifies an invalid key.", storageConnectionString);
      LOG.error("Please confirm the AccountName and AccountKey in the connection string are valid.");
      throw new SamzaException(e);
    }
  }

  public CloudBlobClient getBlobClient() {
    return blobClient;
  }

  public CloudTableClient getTableClient() {
    return tableClient;
  }
}
