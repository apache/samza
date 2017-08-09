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

import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;

/**
 * Config class for reading all user defined parameters for Azure driven coordination services.
 */
public class AzureConfig extends MapConfig {

  // Connection string for Azure Storage Account, format: "DefaultEndpointsProtocol=<https>;AccountName=<>;AccountKey=<>"
  public static final String AZURE_STORAGE_CONNECT = "azure.storage.connect";
  public static final String AZURE_PAGEBLOB_LENGTH = "job.coordinator.azure.blob.length";
  public static final long DEFAULT_AZURE_PAGEBLOB_LENGTH = 5120000;

  private static String containerName;
  private static String blobName;
  private static String tableName;

  public AzureConfig(Config config) {
    super(config);
    ApplicationConfig appConfig = new ApplicationConfig(config);
    //Remove all non-alphanumeric characters from id as table name does not allow them.
    String id = appConfig.getGlobalAppId().replaceAll("[^A-Za-z0-9]", "");
    containerName = "samzacontainer" + id;
    blobName = "samzablob" + id;
    tableName = "samzatable" + id;
  }

  public String getAzureConnect() {
    if (!containsKey(AZURE_STORAGE_CONNECT)) {
      throw new ConfigException("Missing " + AZURE_STORAGE_CONNECT + " config!");
    }
    return get(AZURE_STORAGE_CONNECT);
  }

  public String getAzureContainerName() {
    return containerName;
  }

  public String getAzureBlobName() {
    return blobName;
  }

  public long getAzureBlobLength() {
    return getLong(AZURE_PAGEBLOB_LENGTH, DEFAULT_AZURE_PAGEBLOB_LENGTH);
  }

  public String getAzureTableName() {
    return tableName;
  }
}