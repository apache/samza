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
package org.apache.samza.zk;

import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.Config;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.runtime.LocationId;

import java.util.Collection;
import java.util.Map;

/**
 * An implementation of the {@link MetadataStore} interface where the
 * metadata of the samza application is stored in zookeeper.
 *
 * LocationId for each task is stored in an persistent node in zookeeper.
 * Task locality is stored in the following format in zookeeper:
 *
 * - {zkBaseRootPath/$appName-$appId-$JobName-$JobId-$stageId/}
 *    - localityData
 *        - task01/
 *           locationId1(stored as value in the task zookeeper node)
 *        - task02/
 *           locationId2(stored as value in the task zookeeper node)
 *        ...
 *        - task0N/
 *           locationIdN(stored as value in the task zookeeper node)
 *
 * LocationId for each processor is stored in an ephemeral node in zookeeper.
 * Processor locality is stored in the following format in zookeeper:
 *
 * - {zkBaseRootPath/$appName-$appId-$JobName-$JobId-$stageId/}
 *    - processors/
 *        - processor.000001/
 *            locatoinId1(stored as value in processor zookeeper node)
 *        - processor.000002/
 *            locationId2(stored as value in processor zookeeper node)
 *
 */
public class ZkMetadataStore implements MetadataStore {

  private final Config config;

  private final MetricsRegistry metricsRegistry;

  private final ZkUtils zkUtils;

  public ZkMetadataStore(Config config, MetricsRegistry metricsRegistry) {
    this.config = config;
    this.metricsRegistry = metricsRegistry;
    zkUtils = getZkUtils(this.config, this.metricsRegistry);
  }

  private ZkUtils getZkUtils(Config config, MetricsRegistry metricsRegistry) {
    ZkConfig zkConfig = new ZkConfig(config);
    ZkKeyBuilder keyBuilder = new ZkKeyBuilder(ZkJobCoordinatorFactory.getJobCoordinationZkPath(config));
    ZkClient zkClient = ZkCoordinationUtilsFactory
        .createZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());
    return new ZkUtils(keyBuilder, zkClient, zkConfig.getZkConnectionTimeoutMs(), metricsRegistry);
  }

  @Override
  public void init() {
    zkUtils.connect();
  }

  @Override
  public Map<TaskName, LocationId> readTaskLocality() {
    return zkUtils.readTaskLocality();
  }

  @Override
  public Map<String, LocationId> readProcessorLocality() {
    return zkUtils.readProcessorLocality();
  }

  @Override
  public void writeTaskLocality(String processorId, Map<TaskName, LocationId> taskLocality, String jmxUrl, String jmxTunnelingUrl) {
    zkUtils.writeTaskLocality(taskLocality);
  }

  @Override
  public void deleteTaskLocality(Collection<TaskName> taskNames) {
    zkUtils.deleteAllTaskLocality(taskNames);
  }

  @Override
  public void close() {
    zkUtils.close();
  }
}
