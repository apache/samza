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

import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.CoordinationService;
import org.apache.samza.coordinator.ProcessorLatch;
import org.apache.samza.coordinator.leaderelection.LeaderElector;
import org.apache.samza.zk.ZkLeaderElector;
import org.apache.samza.zk.ZkProcessorLatch;
import org.apache.samza.zk.ZkUtils;


public class ZkCoordinationService implements CoordinationService {
  public final ZkConfig zkConfig;
  public final ZkUtils zkUtils;
  public final String processorIdStr;

  public ZkCoordinationService(String processorId, ZkConfig zkConfig, ZkUtils zkUtils) {
    this.zkConfig = zkConfig;
    this.zkUtils = zkUtils;
    this.processorIdStr = processorId;
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public LeaderElector getLeaderElector() {
    return new ZkLeaderElector(processorIdStr, zkUtils);
  }

  @Override
  public ProcessorLatch getLatch(int size, String latchId) {
    return new ZkProcessorLatch(size, latchId, processorIdStr, zkConfig, zkUtils);
  }
}
