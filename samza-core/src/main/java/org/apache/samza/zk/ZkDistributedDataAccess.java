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

import java.util.HashMap;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.coordinator.DistributedDataAccess;
import org.apache.samza.coordinator.DistributedDataWatcher;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides access to data in ZooKeeper.
 *
 * For each path in ZooKeeper that is accessed, the provided DistributedDataWatcher
 * and a ZkDistributedDataChangeHandler are registered to watch for data changes at the path.
 * When data at a path in ZK changes, ZkDistributedDataChangeHandler is notified and it in turn notifies the
 * assoicated DistributedDataWatcher.
 *
 * ZkDistributedDataAccess also listens to the of the ZooKeeper connection and
 * notifies of session changes through the provided DistributedDataWatcher.
 */
public class ZkDistributedDataAccess implements DistributedDataAccess {
  public static final Logger LOG = LoggerFactory.getLogger(ZkDistributedDataAccess.class);
  private final ZkUtils zkUtils;
  private final ZkKeyBuilder keyBuilder;
  private HashMap<String, Pair<DistributedDataWatcher,ZkDistributedDataChangeHandler>> watchers = new HashMap<>();

  public ZkDistributedDataAccess(ZkUtils zkUtils) {
    if (zkUtils == null) {
      throw new RuntimeException("Cannot operate ZkDistributedDataAccess without ZkUtils.");
    }
    this.zkUtils = zkUtils;
    this.keyBuilder = zkUtils.getKeyBuilder();
    zkUtils.getZkClient().subscribeStateChanges(new ZkSessionStateChangedListener());
  }

  public Object readData(String key, DistributedDataWatcher watcher) {
    String absolutePath = keyBuilder.getRootPath() + "/" + key;
    ZkDistributedDataChangeHandler zkHandler = new ZkDistributedDataChangeHandler(zkUtils);
    this.watchers.put(absolutePath, Pair.of(watcher, zkHandler));
    zkUtils.getZkClient().subscribeDataChanges(absolutePath, zkHandler);
    if(!zkUtils.getZkClient().exists(absolutePath)) {
      return null;
    }
    return zkUtils.getZkClient().readData(absolutePath);
  }

  public void writeData(String key, Object data, DistributedDataWatcher watcher) {
    String absolutePath = keyBuilder.getRootPath() + "/" + key;
    ZkDistributedDataChangeHandler zkHandler = new ZkDistributedDataChangeHandler(zkUtils);
    this.watchers.put(absolutePath, Pair.of(watcher, zkHandler));
    zkUtils.getZkClient().subscribeDataChanges(absolutePath, zkHandler);
    zkUtils.validatePaths(new String[]{absolutePath});
    zkUtils.writeData(absolutePath, data);
    return;
  }

  /**
   * Defines a specific implementation of {@link ZkUtils.GenerationAwareZkDataListener} to listen to data changes in ZK
   */
  class ZkDistributedDataChangeHandler extends ZkUtils.GenerationAwareZkDataListener {

    public ZkDistributedDataChangeHandler(ZkUtils zkUtils) {
      super(zkUtils, "ZkDistributedDataChangeHandler");
    }

    @Override
    public void doHandleDataChange(String dataPath, Object data) {
      DistributedDataWatcher watcher = watchers.get(dataPath).getLeft();
      watcher.handleDataChange(data);
    }

    @Override
    public void doHandleDataDeleted(String dataPath) {
      DistributedDataWatcher watcher = watchers.get(dataPath).getLeft();
      watcher.handleDataDeleted();
    }
  }

  /**
   * Defines a specific implementation of {@link IZkStateListener} to listen to ZK session state changes
   */
  class ZkSessionStateChangedListener implements IZkStateListener {
    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) {
      switch (state) {
        case Expired:
          LOG.info("Got ZK session expiry event.");
          // if the session has expired it means that zookeeper removed all the zk-watches
          // remove them from zkClient also
          watchers.forEach((path,watchers) -> {zkUtils.getZkClient().unsubscribeDataChanges(path,watchers.getRight());} );
        default:
          // received Disconnected, AuthFailed, SyncConnected, ConnectedReadOnly, and SaslAuthenticated. NoOp
          LOG.info("Got ZK event {}. Continue", state.toString());
      }
    }

    /**
     * re-register all the zk-watches with zkClient
     * notify watcher in LAR that data might have possibly changed
     */
    @Override
    public void handleNewSession() {
      LOG.info("Got ZK handleNewSession event.");
      watchers.forEach((path,watchers) -> {
        zkUtils.getZkClient().subscribeDataChanges(path,watchers.getRight());
        watchers.getLeft().handleDataChange(zkUtils.getZkClient().readData(path));
      } );
    }

    @Override
    public void handleSessionEstablishmentError(Throwable error) {
      LOG.info("Got ZK handleSessionEstablishmentError event.");
    }
  }
}