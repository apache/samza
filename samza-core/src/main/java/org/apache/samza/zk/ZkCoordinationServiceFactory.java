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
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.CoordinationServiceFactory;
import org.apache.samza.coordinator.CoordinationUtils;


public class ZkCoordinationServiceFactory implements CoordinationServiceFactory {
  // TODO - Why should this method be synchronized?
  synchronized public CoordinationUtils getCoordinationService(String groupId, String participantId, Config config) {
    ZkConfig zkConfig = new ZkConfig(config);
    ZkClient zkClient = ZkUtils.createZkClient(zkConfig);
    ZkUtils zkUtils = new ZkUtils(new ZkKeyBuilder(groupId), zkClient, zkConfig.getZkConnectionTimeoutMs());
    return new ZkCoordinationUtils(participantId, zkConfig, zkUtils, new ScheduleAfterDebounceTime());
  }

}
