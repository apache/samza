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

import com.google.common.base.Preconditions;
import org.apache.samza.coordinator.ClusterMembership;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkClusterMembership implements ClusterMembership {

  public static final Logger LOG = LoggerFactory.getLogger(ZkClusterMembership.class);
  public static final String PROCESSORS_PATH = "processors";
  private final ZkUtils zkUtils;
  private final String processorsPath;
  private final String participantId;
  private final ZkKeyBuilder keyBuilder;

  public ZkClusterMembership(String participantId, ZkUtils zkUtils) {
    Preconditions.checkNotNull(participantId, "ParticipantId cannot be null");
    Preconditions.checkNotNull(zkUtils, "ZkUtils cannot be null");
    this.zkUtils = zkUtils;
    this.participantId = participantId;
    this.keyBuilder = zkUtils.getKeyBuilder();
    processorsPath = String.format("%s/%s", keyBuilder.getRootPath(), PROCESSORS_PATH);
    zkUtils.validatePaths(new String[] {processorsPath});
  }

  @Override
  public String registerProcessor() {
    String nodePath = zkUtils.getZkClient().createEphemeralSequential(processorsPath + "/", participantId);
    LOG.info("created ephemeral node. Registered the processor in the cluster.");
    return ZkKeyBuilder.parseIdFromPath(nodePath);
  }

  @Override
  public int getNumberOfProcessors() {
    return zkUtils.getZkClient().getChildren(processorsPath).size();
  }

  @Override
  public void unregisterProcessor(String processorId) {
    if (processorId == null) {
      LOG.warn("Can not unregister processor with null processorId");
      return;
    }
    String nodePath = processorsPath + "/" + processorId;
    if (zkUtils.exists(nodePath)) {
      zkUtils.getZkClient().delete(nodePath);
      LOG.info("Ephemeral node deleted. Unregistered the processor from cluster membership.");
    } else {
      LOG.warn("Ephemeral node you want to delete doesnt exist. Processor with id {} is not currently registered.", processorId);
    }
  }
}