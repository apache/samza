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

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ZkUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ZkUtils.class);

  private final ZkStateChangeHandler zkStateChangeHandler;
  private final ZkClient zkClient;
  private final ZkConnection zkConnnection;
  private volatile String ephemeralPath = null;
  private final ZkKeyBuilder keyBuilder;
  private final int connectionTimeoutMs;
  private final String processorId;

  public ZkUtils(ZkKeyBuilder zkKeyBuilder, String zkConnectString, String processorId, int sessionTimeoutMs, int connectionTimeoutMs) {
    this.keyBuilder = zkKeyBuilder;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.zkConnnection = new ZkConnection(zkConnectString, sessionTimeoutMs);
    this.zkClient = new ZkClient(zkConnnection, this.connectionTimeoutMs);
    this.zkStateChangeHandler = new ZkStateChangeHandler();
    this.processorId = processorId;
  }

  public void connect() throws ZkInterruptedException {
    boolean isConnected = zkClient.waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS);
    if (!isConnected) {
      throw new RuntimeException("Unable to connect to Zookeeper within connectionTimeout " + connectionTimeoutMs + "ms. Shutting down!");
    } else {
      zkClient.subscribeStateChanges(zkStateChangeHandler);
    }
  }

  public ZkClient getZkClient() {
    return zkClient;
  }

  public ZkConnection getZkConnnection() {
    return zkConnnection;
  }

  public ZkKeyBuilder getKeyBuilder() {
    return keyBuilder;
  }

  public void makeSurePersistentPathsExists(String[] paths) {
    for (String path : paths) {
      if (!zkClient.exists(path)) {
        zkClient.createPersistent(path, true);
      }
    }
  }

  public synchronized String registerProcessorAndGetId() {
    try {
      // TODO: Data should be more than just the hostname. Use Json serialized data
      ephemeralPath =
          zkClient.createEphemeralSequential(keyBuilder.getProcessorsPath() + "/processor-", InetAddress.getLocalHost().getHostName());
      return ephemeralPath;
    } catch (UnknownHostException e) {
      throw new RuntimeException("Failed to register as worker. Aborting...");
    }
  }

  public synchronized String getEphemeralPath() {
    return ephemeralPath;
  }

  public List<String> getActiveProcessors() {
    List<String> children = zkClient.getChildren(keyBuilder.getProcessorsPath());
    assert children.size() > 0;
    Collections.sort(children);
    LOG.info("Found these children - " + children);
    return children;
  }

  public void subscribeToProcessorChange(IZkChildListener listener) {
    LOG.info("pid=" + processorId + " subscribing for child change at:" + keyBuilder.getProcessorsPath());
    zkClient.subscribeChildChanges(keyBuilder.getProcessorsPath(), listener);
  }

  /* Wrapper for standard I0Itec methods */
  public void unsubscribeDataChanges(String path, IZkDataListener dataListener) {
    LOG.info("pid=" + processorId + " unsubscribing for data change at:" + path);
    zkClient.unsubscribeDataChanges(path, dataListener);
  }

  public void subscribeDataChanges(String path, IZkDataListener dataListener) {
    LOG.info("pid=" + processorId + " subscribing for data change at:" + path);
    zkClient.subscribeDataChanges(path, dataListener);
  }

  public boolean exists(String path) {
    return zkClient.exists(path);
  }

  public void close() {
    try {
      zkConnnection.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    zkClient.close();
  }

  class ZkStateChangeHandler implements IZkStateListener {
    public ZkStateChangeHandler() {
    }

    /**
     * Called when the zookeeper connection state has changed.
     *
     * @param state The new state.
     * @throws Exception On any error.
     */
    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception On any error.
     */
    @Override
    public void handleNewSession() throws Exception {

    }

    /**
     * Called when a session cannot be re-established. This should be used to implement connection
     * failure handling e.g. retry to connect or pass the error up
     *
     * @param error The error that prevents a session from being established
     * @throws Exception On any error.
     */
    @Override
    public void handleSessionEstablishmentError(Throwable error) throws Exception {

    }
  }
}
