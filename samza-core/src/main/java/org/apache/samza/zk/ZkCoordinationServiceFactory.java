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
import org.apache.zookeeper.client.ConnectStringParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkCoordinationServiceFactory implements CoordinationServiceFactory {
  public final static Logger LOG = LoggerFactory.getLogger(ZkCoordinationServiceFactory.class);

  // TODO - Why should this method be synchronized?
  synchronized public CoordinationUtils getCoordinationService(String groupId, String participantId, Config config) {
    ZkConfig zkConfig = new ZkConfig(config);

    ZkClient zkClient = createZkClient(zkConfig.getZkConnect(),
        zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());

    // make sure the 'path' exists
    createZkPath(zkConfig.getZkConnect(), zkClient);

    ZkUtils zkUtils = new ZkUtils(new ZkKeyBuilder(groupId), zkClient, zkConfig.getZkConnectionTimeoutMs());

    return new ZkCoordinationUtils(participantId, zkConfig, zkUtils, new ScheduleAfterDebounceTime());
  }

  /**
   * helper method to create zkClient
   * @param connectString - zkConnect string
   * @param sessionTimeoutMS - session timeout
   * @param connectionTimeoutMs - connection timeout
   * @return zkClient object
   */
  public static ZkClient createZkClient(String connectString, int sessionTimeoutMS, int connectionTimeoutMs) {
    try {
      return new ZkClient(connectString, sessionTimeoutMS, connectionTimeoutMs);
    } catch (Exception e) {
      // ZkClient constructor may throw a variety of different exceptions, not all of them Zk based.
      throw new SamzaException("zkClient failed to connect to ZK at :" + connectString, e);
    }
  }

  /**
   * if ZkConnectString contains some path at the end, it needs to be created when connecting for the first time.
   * @param zkConnect - connect string
   * @param zkClient - zkClient object to talk to the ZK
   */
  public static void createZkPath(String zkConnect, ZkClient zkClient) {
    ConnectStringParser parser = new ConnectStringParser(zkConnect);

    String path = parser.getChrootPath();
    LOG.info("path =" + path);
    if (!Strings.isNullOrEmpty(path)) {
      // create this path in zk
      LOG.info("first connect. creating path =" + path + " in ZK " + parser.getServerAddresses());
      if (!zkClient.exists(path)) {
        zkClient.createPersistent(path, true); // will create parents if needed and will not throw exception if exists
      }
    }
  }

}
