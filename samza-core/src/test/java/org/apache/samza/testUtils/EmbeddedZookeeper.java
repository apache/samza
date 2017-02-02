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
package org.apache.samza.testUtils;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;

public class EmbeddedZookeeper {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedZookeeper.class);

  private static final String SNAPSHOT_DIR_RELATIVE_PATH = "zk/snapshot";
  private static final String LOG_DIR_RELATIVE_PATH = "zk/log";
  private static final int TICK_TIME = 500;
  private static final int MAX_CLIENT_CONNECTIONS = 1024;
  private static final int RANDOM_PORT = 0;

  private ZooKeeperServer zooKeeperServer = null;
  private ServerCnxnFactory serverCnxnFactory = null;
  private File snapshotDir = null;
  private File logDir = null;

  public void setup() {
    try {
      snapshotDir = FileUtil.createFileInTempDir(SNAPSHOT_DIR_RELATIVE_PATH);
      logDir = FileUtil.createFileInTempDir(LOG_DIR_RELATIVE_PATH);
    } catch (IOException e) {
      LOGGER.error("Failed to setup Zookeeper Server Environment", e);
      Assert.fail("Failed to setup Zookeeper Server Environment");
    }

    try {
      zooKeeperServer = new ZooKeeperServer(snapshotDir, logDir, TICK_TIME);
      serverCnxnFactory = NIOServerCnxnFactory.createFactory();
      InetSocketAddress addr = new InetSocketAddress("127.0.0.1", RANDOM_PORT);
      serverCnxnFactory.configure(addr, MAX_CLIENT_CONNECTIONS);

      serverCnxnFactory.startup(zooKeeperServer);
    } catch (Exception e) {
      LOGGER.error("Zookeeper Server failed to start", e);
      Assert.fail("Zookeeper Server failed to start");
    }
  }

  public void teardown() {
    serverCnxnFactory.shutdown();

    try {
      serverCnxnFactory.join();
    } catch (InterruptedException e) {
      LOGGER.warn("Zookeeper server may not have terminated cleanly!", e);
    }

    try {
      FileUtil.deleteDir(snapshotDir);
      FileUtil.deleteDir(logDir);
    } catch (FileNotFoundException | NullPointerException e) {
      LOGGER.warn("Zookeeper Server Environment Cleanup failed!", e);
    }
  }

  public int getPort() {
    return zooKeeperServer.getClientPort();
  }

  public static void main(String[] args) {
    EmbeddedZookeeper zk = new EmbeddedZookeeper();
    zk.setup();
    System.out.println("Zk Server Started!!");
    try {
      Thread.sleep(86400);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    zk.teardown();
  }

}
