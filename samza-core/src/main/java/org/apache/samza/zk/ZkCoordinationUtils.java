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

import com.google.common.base.Strings;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.CoordinationLock;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.Latch;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.apache.zookeeper.client.ConnectStringParser;
import org.slf4j.Logger;


public class ZkCoordinationUtils implements CoordinationUtils {
  private final String participantId;
  private final ZkUtils zkUtils;
  private final String groupId;

  private final static Logger LOG = org.slf4j.LoggerFactory.getLogger(ZkCoordinationUtils.class);

  public ZkCoordinationUtils(String groupId, String participantId, Config config) {

    this.participantId = participantId;
    this.groupId = groupId;
    ZkConfig zkConfig = new ZkConfig(config);

    ZkClient zkClient = createZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());

    this.zkUtils = new ZkUtils(new ZkKeyBuilder(groupId), zkClient, zkConfig.getZkConnectionTimeoutMs(), new NoOpMetricsRegistry());
    LOG.info("Created ZkCoordinationUtils. zkUtils = " + zkUtils + "; groupid=" + groupId);
  }

  @Override
  public LeaderElector getLeaderElector() {
    LOG.info("getting LE. participant=" + participantId + ";zkutils=" + zkUtils);
    ZkLeaderElector le = new ZkLeaderElector(participantId, zkUtils);
    LOG.info("got LE=" + le);
    return le;
  }

  @Override
  public Latch getLatch(int size, String latchId) {
    LOG.info("getting latch: size=" + size + ";id = " + latchId);
    Latch l = new ZkProcessorLatch(size, latchId, participantId, zkUtils);
    LOG.info("got latch:" + l);
    return l;
  }

  @Override
  public CoordinationLock getLock() {
    return new ZkCoordinationLock(groupId, participantId, zkUtils);
  }

  // static auxiliary methods
  public static ZkClient createZkClient(String connectString, int sessionTimeoutMS, int connectionTimeoutMs) {
    ZkClient zkClient;
    try {
      zkClient = new ZkClient(connectString, sessionTimeoutMS, connectionTimeoutMs);
    } catch (Exception e) {
      // ZkClient constructor may throw a variety of different exceptions, not all of them Zk based.
      throw new SamzaException("zkClient failed to connect to ZK at :" + connectString, e);
    }

    // make sure the namespace in zk exists (if specified)
    validateZkNameSpace(connectString, zkClient);

    return zkClient;
  }
  /**
   * if ZkConnectString contains namespace path at the end, but it does not exist we should fail
   * @param zkConnect - connect string
   * @param zkClient - zkClient object to talk to the ZK
   */
  public static void validateZkNameSpace(String zkConnect, ZkClient zkClient) {
    ConnectStringParser parser = new ConnectStringParser(zkConnect);

    String path = parser.getChrootPath();
    if (Strings.isNullOrEmpty(path)) {
      return; // no namespace path
    }

    //LOG.info("connectString = " + zkConnect + "; path =" + path);

    // if namespace specified (path above) but "/" does not exists, we will fail
    if (!zkClient.exists("/")) {
      throw new SamzaException("Zookeeper namespace: " + path + " does not exist for zk at " + zkConnect);
    }
  }
}
