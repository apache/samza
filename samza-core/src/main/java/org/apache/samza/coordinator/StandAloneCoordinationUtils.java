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

import com.google.common.base.Strings;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.apache.samza.zk.ZkJobCoordinatorFactory;
import org.apache.samza.zk.ZkKeyBuilder;
import org.apache.samza.zk.ZkLeaderElector;
import org.apache.samza.zk.ZkProcessorLatch;
import org.apache.samza.zk.ZkUtils;
import org.apache.zookeeper.client.ConnectStringParser;
import org.slf4j.Logger;


public class StandAloneCoordinationUtils {
  private final String participantId;
  private final ZkUtils zkUtils;

  private final static Logger LOG = org.slf4j.LoggerFactory.getLogger(StandAloneCoordinationUtils.class);

  public StandAloneCoordinationUtils(String groupId, String participantId, Config config) {

    // currently we figure out if it is ZK based utilities by checking JobCoordinatorConfig.JOB_COORDINATOR_FACTORY
    String jobCoordinatorFactoryClassName = config.get(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "");

    if (!ZkJobCoordinatorFactory.class.getName().equals(jobCoordinatorFactoryClassName))
      throw new SamzaException(String.format("Samza supports only ZK based coordination utilities with %s == %s",
          JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, ZkJobCoordinatorFactory.class.getName()));

    // create ZK based utils
    this.participantId = participantId;
    ZkConfig zkConfig = new ZkConfig(config);

    ZkClient zkClient = createZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());

    this.zkUtils = new ZkUtils(new ZkKeyBuilder(groupId), zkClient, zkConfig.getZkConnectionTimeoutMs(), new NoOpMetricsRegistry());
    LOG.info("Created CoordinationUtis1. zkUtils = " + zkUtils + "; groupid=" + groupId);
  }

  public LeaderElector getLeaderElector() {
    LOG.info("getting LE. participant=" + participantId + ";zkutils=" + zkUtils);
    ZkLeaderElector le = new ZkLeaderElector(participantId, zkUtils);
    LOG.info("got LE=" + le);
    return le;
  }

  public Latch getLatch(int size, String latchId) {
    LOG.info("getting latch: size=" + size + ";id = " + latchId);
    Latch l = new ZkProcessorLatch(size, latchId, participantId, zkUtils);
    LOG.info("got latch:" + l);
    return l;
  }

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
