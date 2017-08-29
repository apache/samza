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

package org.apache.samza.coordinator;

import org.apache.samza.AzureClient;
import org.apache.samza.config.AzureConfig;
import org.apache.samza.config.Config;
import org.apache.samza.util.BlobUtils;


public class AzureCoordinationUtils implements CoordinationUtils {

  private final AzureConfig azureConfig;
  private final AzureClient client;

  public AzureCoordinationUtils(Config config) {
    azureConfig = new AzureConfig(config);
    this.client = new AzureClient(azureConfig.getAzureConnect());
  }

  @Override
  public LeaderElector getLeaderElector() throws UnsupportedOperationException {
    return null;
  }

  @Override
  public Latch getLatch(int size, String latchId) throws UnsupportedOperationException {
    return null;
  }

  @Override
  public DistributedLockWithState getLockWithState(String lockId) {
    BlobUtils blob = new BlobUtils(client, azureConfig.getAzureContainerName(),
        azureConfig.getAzureBlobName() + lockId, azureConfig.getAzureBlobLength());
    return new AzureLock(blob);
  }

  @Override
  public void close() {

  }
}
