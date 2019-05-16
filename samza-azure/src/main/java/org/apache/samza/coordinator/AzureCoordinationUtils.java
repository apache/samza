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


public class AzureCoordinationUtils implements CoordinationUtils {

  private final AzureConfig azureConfig;
  private final AzureClient client;

  public AzureCoordinationUtils(Config config) {
    azureConfig = new AzureConfig(config);
    this.client = new AzureClient(azureConfig.getAzureConnectionString());
  }

  @Override
  public LeaderElector getLeaderElector() throws UnsupportedOperationException {
    return null;
  }

  @Override
  public Latch getLatch(int size, String latchId) throws UnsupportedOperationException {
    return null;
  }

  /**
   * To support DistributedLock in Azure, even {@link org.apache.samza.metadatastore.MetadataStore} needs to be implemented.
   * Because, both of these are used in {@link org.apache.samza.execution.LocalJobPlanner} for intermediate stream creation.
   * Currently MetadataStore defaults to ZkMetataStore in LocalJobPlanner due to `metadata.store.factory` not being exposed
   * So in order to avoid using AzureLock coupled with ZkMetadataStore, DistributedLock is not supported for Azure
   * See SAMZA-2180 for more details.
   */
  @Override
  public DistributedLock getLock(String lockId) {
    throw new UnsupportedOperationException("DistributedLock not supported in Azure!");
  }

  @Override
  public ClusterMembership getClusterMembership() {
    throw new UnsupportedOperationException("ClusterMembership not supported in Azure!");
  }

  @Override
  public void close() {
  }
}
