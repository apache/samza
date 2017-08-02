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

import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;


public class AzureConfig extends MapConfig {

  // Connection string for Azure Storage Account, format: "DefaultEndpointsProtocol=<https>;AccountName=<>;AccountKey=<>;"
  public static final String AZURE_STORAGE_CONNECT = "job.coordinator.azure.storage.connect";
  public static final String AZURE_CONTAINER_NAME = "job.coordinator.azure.container.name";
  public static final String AZURE_BLOB_NAME = "job.coordinator.azure.blob.name";
  public static final String AZURE_TABLE_NAME = "job.coordinator.azure.table.name";
  public static final String AZURE_PAGEBLOB_LENGTH = "job.coordinator.azure.blob.length";

  public static final String DEFAULT_AZURE_CONTAINER_NAME = "samzacontainer";
  public static final String DEFAULT_AZURE_BLOB_NAME = "samzablob";
  public static final String DEFAULT_AZURE_TABLE_NAME = "samzatable";
  public static final long DEFAULT_AZURE_PAGEBLOB_LENGTH = 5120000;

  public AzureConfig(Config config) {
    super(config);
  }

  public String getAzureConnect() {
    if (!containsKey(AZURE_STORAGE_CONNECT)) {
      throw new ConfigException("Missing " + AZURE_STORAGE_CONNECT + " config!");
    }
    return get(AZURE_STORAGE_CONNECT);
  }

  public String getAzureContainerName() {
    return get(AZURE_CONTAINER_NAME, DEFAULT_AZURE_CONTAINER_NAME);
  }

  public String getAzureBlobName() {
    return get(AZURE_BLOB_NAME, DEFAULT_AZURE_BLOB_NAME);
  }
  public long getAzureBlobLength() {
    return getLong(AZURE_PAGEBLOB_LENGTH, DEFAULT_AZURE_PAGEBLOB_LENGTH);
  }

  public String getAzureTableName() {
    return get(AZURE_TABLE_NAME, DEFAULT_AZURE_TABLE_NAME);
  }

}

