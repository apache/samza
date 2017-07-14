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
import org.apache.samza.coordinator.CoordinationServiceFactory;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.apache.zookeeper.client.ConnectStringParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkCoordinationServiceFactory implements CoordinationServiceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ZkCoordinationServiceFactory.class);

  public CoordinationUtils getCoordinationService(String groupId, String participantId, Config config) {
    ZkConfig zkConfig = new ZkConfig(config);

    ZkClient zkClient =
        createZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());

    ZkUtils zkUtils = new ZkUtils(new ZkKeyBuilder(groupId), zkClient, zkConfig.getZkConnectionTimeoutMs(), new NoOpMetricsRegistry());

    return new ZkCoordinationUtils(participantId, zkConfig, zkUtils);
  }

  /**
   * helper method to create zkClient
   * @param connectString - zkConnect string
   * @param sessionTimeoutMS - session timeout
   * @param connectionTimeoutMs - connection timeout
   * @return zkClient object
   */
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

    LOG.info("connectString = " + zkConnect + "; path =" + path);

    // if namespace specified (path above) but "/" does not exists, we will fail
    if (!zkClient.exists("/")) {
      throw new SamzaException("Zookeeper namespace: " + path + " does not exist for zk at " + zkConnect);
    }
  }
}
