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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class to help manage Zk connection and ZkClient.
 * It also provides additional utility methods for read/write/subscribe/unsubscribe access to the ZK tree.
 *
 * <p>
 *  <b>Note on ZkClient:</b>
 *  {@link ZkClient} consists of two threads - I/O thread and Event thread.
 *  I/O thread manages heartbeats to the Zookeeper server in the ensemble and handles responses to synchronous methods
 *  in Zookeeper API.
 *  Event thread typically receives all the Watcher events and delivers to registered listeners. It, also, handles
 *  responses to asynchronous methods in Zookeeper API.
 * </p>
 *
 * <p>
 *   <b>Note on Session Management:</b>
 *   Session management, if needed, should be handled by the caller. This can be done by implementing
 *   {@link org.I0Itec.zkclient.IZkStateListener} and subscribing this listener to the current ZkClient. Note: The connection state change
 *   callbacks are invoked in the context of the Event thread of the ZkClient. So, it is advised to do non-blocking
 *   processing in the callbacks.
 * </p>
 */
public class ZkUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ZkUtils.class);

  private final ZkClient zkClient;
  private volatile String ephemeralPath = null;
  private final ZkKeyBuilder keyBuilder;
  private final int connectionTimeoutMs;
  private ZkJobCoordinatorMetrics metrics;

  public ZkUtils(ZkKeyBuilder zkKeyBuilder, ZkClient zkClient, int connectionTimeoutMs) {
    this.keyBuilder = zkKeyBuilder;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.zkClient = zkClient;
  }

  public ZkUtils(ZkKeyBuilder zkKeyBuilder, ZkClient zkClient, int connectionTimeoutMs, ZkJobCoordinatorMetrics metrics) {
    this.keyBuilder = zkKeyBuilder;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.zkClient = zkClient;
    this.metrics = metrics;
  }

  public void connect() throws ZkInterruptedException {
    boolean isConnected = zkClient.waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS);
    if (!isConnected) {
      metrics.zkConnectionError.inc();
      throw new RuntimeException("Unable to connect to Zookeeper within connectionTimeout " + connectionTimeoutMs + "ms. Shutting down!");
    }
  }

  public static ZkConnection createZkConnection(String zkConnectString, int sessionTimeoutMs) {
    return new ZkConnection(zkConnectString, sessionTimeoutMs);
  }

  ZkClient getZkClient() {
    return zkClient;
  }

  public ZkKeyBuilder getKeyBuilder() {
    return keyBuilder;
  }

  /**
   * Returns a ZK generated identifier for this client.
   * If the current client is registering for the first time, it creates an ephemeral sequential node in the ZK tree
   * If the current client has already registered and is still within the same session, it returns the already existing
   * value for the ephemeralPath
   *
   * @param data Object that should be written as data in the registered ephemeral ZK node
   * @return String representing the absolute ephemeralPath of this client in the current session
   */
  public synchronized String registerProcessorAndGetId(final ProcessorData data) {
    if (ephemeralPath == null) {
      ephemeralPath =
          zkClient.createEphemeralSequential(
              keyBuilder.getProcessorsPath() + "/", data.toString());

      LOG.info("newly generated path for " + data +  " is " +  ephemeralPath);
      return ephemeralPath;
    } else {
      LOG.info("existing path for " + data +  " is " +  ephemeralPath);
      return ephemeralPath;
    }
  }

  /**
   * Method is used to get the <i>sorted</i> list of currently active/registered processors (znodes)
   *
   * @return List of absolute ZK node paths
   */
  public List<String> getSortedActiveProcessorsZnodes() {
    List<String> znodeIds = zkClient.getChildren(keyBuilder.getProcessorsPath());
    if (znodeIds.size() > 0) {
      Collections.sort(znodeIds);
      LOG.info("Found these children - " + znodeIds);
    }
    return znodeIds;
  }

  /**
   * Method is used to read processor's data from the znode
   * @param fullPath absolute path to the znode
   * @return processor's data
   * @throws SamzaException when fullPath doesn't exist in zookeeper
   * or problems with connecting to zookeeper.
   */
  String readProcessorData(String fullPath) {
    try {
      String data = zkClient.readData(fullPath, false);
      metrics.reads.inc();
      return data;
    } catch (Exception e) {
      throw new SamzaException(String.format("Cannot read ZK node: %s", fullPath), e);
    }
  }

  /**
   * Method is used to get the list of currently active/registered processor ids
   * @return List of processorIds
   */
  public List<String> getSortedActiveProcessorsIDs() {
    return getActiveProcessorsIDs(getSortedActiveProcessorsZnodes());
  }

  /**
   * Method is used to get the <i>sorted</i> list of processors ids for a given list of znodes
   * @param znodeIds - list of relative paths of the children's znodes
   * @return List of processor ids for a given list of znodes
   */
  public List<String> getActiveProcessorsIDs(List<String> znodeIds) {
    String processorPath = keyBuilder.getProcessorsPath();
    List<String> processorIds = new ArrayList<>(znodeIds.size());
    if (znodeIds.size() > 0) {

      for (String child : znodeIds) {
        String fullPath = String.format("%s/%s", processorPath, child);
        processorIds.add(new ProcessorData(readProcessorData(fullPath)).getProcessorId());
      }

      LOG.info("Found these children - " + znodeIds);
      LOG.info("Found these processorIds - " + processorIds);
    }
    return processorIds;
  }

  /* Wrapper for standard I0Itec methods */
  public void unsubscribeDataChanges(String path, IZkDataListener dataListener) {
    zkClient.unsubscribeDataChanges(path, dataListener);
  }

  public void subscribeDataChanges(String path, IZkDataListener dataListener) {
    zkClient.subscribeDataChanges(path, dataListener);
    metrics.subscriptions.inc();
  }

  public void subscribeChildChanges(String path, IZkChildListener listener) {
    zkClient.subscribeChildChanges(path, listener);
    metrics.subscriptions.inc();
  }

  public void unsubscribeChildChanges(String path, IZkChildListener childListener) {
    zkClient.unsubscribeChildChanges(path, childListener);
  }

  public void writeData(String path, Object object) {
    zkClient.writeData(path, object);
    metrics.writes.inc();
  }

  public boolean exists(String path) {
    return zkClient.exists(path);
  }

  public void close() throws ZkInterruptedException {
    zkClient.close();
  }

  /**
    * subscribe for changes of JobModel version
    * @param dataListener describe this
    */
  public void subscribeToJobModelVersionChange(IZkDataListener dataListener) {
    LOG.info(" subscribing for jm version change at:" + keyBuilder.getJobModelVersionPath());
    zkClient.subscribeDataChanges(keyBuilder.getJobModelVersionPath(), dataListener);
    metrics.subscriptions.inc();
  }

  /**
   * Publishes new job model into ZK.
   * This call should FAIL if the node already exists.
   * @param jobModelVersion  version of the jobModeL to publish
   * @param jobModel jobModel to publish
   *
   */
  public void publishJobModel(String jobModelVersion, JobModel jobModel) {
    try {
      ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
      String jobModelStr = mmapper.writerWithDefaultPrettyPrinter().writeValueAsString(jobModel);
      LOG.info("jobModelAsString=" + jobModelStr);
      zkClient.createPersistent(keyBuilder.getJobModelPath(jobModelVersion), jobModelStr);
      LOG.info("wrote jobModel path =" + keyBuilder.getJobModelPath(jobModelVersion));
    } catch (Exception e) {
      LOG.error("JobModel publish failed for version=" + jobModelVersion, e);
      throw new SamzaException(e);
    }
  }

  /**
   * get the job model from ZK by version
   * @param jobModelVersion jobModel version to get
   * @return job model for this version
   */
  public JobModel getJobModel(String jobModelVersion) {
    LOG.info("read the model ver=" + jobModelVersion + " from " + keyBuilder.getJobModelPath(jobModelVersion));
    Object data = zkClient.readData(keyBuilder.getJobModelPath(jobModelVersion));
    metrics.reads.inc();
    ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
    JobModel jm;
    try {
      jm = mmapper.readValue((String) data, JobModel.class);
    } catch (IOException e) {
      throw new SamzaException("failed to read JobModel from ZK", e);
    }
    return jm;
  }

  /**
   * read the jobmodel version from ZK
   * @return jobmodel version as a string
   */
  public String getJobModelVersion() {
    return zkClient.<String>readData(keyBuilder.getJobModelVersionPath());
  }

  /**
   * publish the version number of the next JobModel
   * @param oldVersion - used to validate, that no one has changed the version in the meanwhile.
   * @param newVersion - new version.
   */
  public void publishJobModelVersion(String oldVersion, String newVersion) {
    Stat stat = new Stat();
    String currentVersion = zkClient.<String>readData(keyBuilder.getJobModelVersionPath(), stat);
    metrics.reads.inc();
    LOG.info("publishing new version: " + newVersion + "; oldVersion = " + oldVersion + "(" + stat
        .getVersion() + ")");

    if (currentVersion != null && !currentVersion.equals(oldVersion)) {
      throw new SamzaException(
          "Someone changed JobModelVersion while the leader was generating one: expected" + oldVersion + ", got " + currentVersion);
    }
    // data version is the ZK version of the data from the ZK.
    int dataVersion = stat.getVersion();
    try {
      stat = zkClient.writeDataReturnStat(keyBuilder.getJobModelVersionPath(), newVersion, dataVersion);
      metrics.writes.inc();
    } catch (Exception e) {
      String msg = "publish job model version failed for new version = " + newVersion + "; old version = " + oldVersion;
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    }
    LOG.info("published new version: " + newVersion + "; expected data version = " + (dataVersion + 1) +
        "(actual data version after update = " + stat.getVersion() + ")");
  }


  /**
   * verify that given paths exist in ZK
   * @param paths - paths to verify or create
   */
  public void makeSurePersistentPathsExists(String[] paths) {
    for (String path : paths) {
      if (!zkClient.exists(path)) {
        zkClient.createPersistent(path, true);
      }
    }
  }

  /**
   * subscribe to the changes in the list of processors in ZK
   * @param listener - will be called when a processor is added or removed.
   */
  public void subscribeToProcessorChange(IZkChildListener listener) {
    LOG.info("subscribing for child change at:" + keyBuilder.getProcessorsPath());
    zkClient.subscribeChildChanges(keyBuilder.getProcessorsPath(), listener);
    metrics.subscriptions.inc();
  }

  /**
   * cleanup old data from ZK
   * @param numVersionsToLeave - number of versions to leave
   */
  public void cleanupZK(int numVersionsToLeave) {
    deleteOldBarrierVersions(numVersionsToLeave);
    deleteOldJobModels(numVersionsToLeave);
  }

  private void deleteOldJobModels(int numVersionsToLeave) {
    // read current list of JMs
    String path = keyBuilder.getJobModelPathPrefix();
    LOG.info("jm_path=" + path);
    List<String> znodeIds = zkClient.getChildren(path);
    deleteOldVersionPath(path, znodeIds, numVersionsToLeave, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        // jm version name format is <num>
        return Integer.valueOf(o1) - Integer.valueOf(o2);
      }
    });
  }

  void deleteOldBarrierVersions(int numVersionsToLeave) {
    // read current list of barriers
    String path = keyBuilder.getJobModelVersionBarrierPrefix();
    LOG.info("jm path=" + path);
    List<String> znodeIds = zkClient.getChildren(path);
    LOG.info("ids = " + znodeIds);
    deleteOldVersionPath(path, znodeIds, numVersionsToLeave,  new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        // barrier's name format is barrier_<num>
        return ZkBarrierForVersionUpgrade.getVersion(o1) - ZkBarrierForVersionUpgrade.getVersion(o2);
      }
    });
  }

  void deleteOldVersionPath(String path, List<String> zNodeIds, int numVersionsToLeave, Comparator<String> c) {
    if (StringUtils.isEmpty(path) || zNodeIds == null) {
      LOG.warn("cannot cleanup empty path or empty list in ZK");
    }
    LOG.info("ids = " + zNodeIds);
    if (zNodeIds.size() > numVersionsToLeave) {
      Collections.sort(zNodeIds, c);
      LOG.info("sorted ids = " + zNodeIds);
      // get the znodes to delete
      int size = zNodeIds.size();
      List<String> zNodesToDelete = zNodeIds.subList(0, numVersionsToLeave);
      LOG.info("starting cleaning of barrier versions. from size=" + size + "to size " + zNodesToDelete.size() + "; num to leave=" +  numVersionsToLeave);
      for (String znodeId: zNodesToDelete) {
        String pathToDelete = path + "/" + znodeId;
        LOG.info(pathToDelete);
        try {
          zkClient.deleteRecursive(pathToDelete);
        } catch (Exception e) {
          LOG.warn("delete of node " + pathToDelete + " failed.", e);
        }
      }
    }
  }
}
