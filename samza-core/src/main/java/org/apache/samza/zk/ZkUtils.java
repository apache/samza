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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
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
  private final String processorId = "TO BE PASSED IN THE CONSTRUCTOR"; //TODO

  public ZkUtils(ZkKeyBuilder zkKeyBuilder, ZkClient zkClient, int connectionTimeoutMs) {
    this.keyBuilder = zkKeyBuilder;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.zkClient = zkClient;
  }

  public void connect() throws ZkInterruptedException {
    boolean isConnected = zkClient.waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS);
    if (!isConnected) {
      throw new RuntimeException("Unable to connect to Zookeeper within connectionTimeout " + connectionTimeoutMs + "ms. Shutting down!");
    }
  }

  public static ZkConnection createZkConnection(String zkConnectString, int sessionTimeoutMs) {
    return new ZkConnection(zkConnectString, sessionTimeoutMs);
  }

  public static ZkClient createZkClient(ZkConnection zkConnection, int connectionTimeoutMs) {
    return new ZkClient(zkConnection, connectionTimeoutMs);
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
  public synchronized String registerProcessorAndGetId(final Object data) {
    if (ephemeralPath == null) {
      // TODO: Data should be more than just the hostname. Use Json serialized data
      ephemeralPath =
          zkClient.createEphemeralSequential(
              keyBuilder.getProcessorsPath() + "/", data);
      return ephemeralPath;
    } else {
      return ephemeralPath;
    }
  }

  public synchronized String getEphemeralPath() {
    return ephemeralPath;
  }

  /**
   * Method is used to get the <i>sorted</i> list of currently active/registered processors
   *
   * @return List of absolute ZK node paths
   */
  public List<String> getSortedActiveProcessors() {
    List<String> children = zkClient.getChildren(keyBuilder.getProcessorsPath());
    if (children.size() > 0) {
      Collections.sort(children);
      LOG.info("Found these children - " + children);
    }
    return children;
  }

  /* Wrapper for standard I0Itec methods */
  public void unsubscribeDataChanges(String path, IZkDataListener dataListener) {
    zkClient.unsubscribeDataChanges(path, dataListener);
  }

  public void subscribeDataChanges(String path, IZkDataListener dataListener) {
    zkClient.subscribeDataChanges(path, dataListener);
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
    LOG.info("pid=" + processorId + " subscribing for jm version change at:" + keyBuilder.getJobModelVersionPath());
    zkClient.subscribeDataChanges(keyBuilder.getJobModelVersionPath(), dataListener);
  }

  /**
   * publishes new job model into ZK
   * @param jobModelVersion  version of the jobModeL to publish
   * @param jobModel jobModel to publish
   */
  public void publishNewJobModel(String jobModelVersion, JobModel jobModel) {
    try {
      // This call should FAIL if the node already exists.
      ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
      String jobModelStr = mmapper.writerWithDefaultPrettyPrinter().writeValueAsString(jobModel);
      LOG.info("pid=" + processorId + " jobModelAsString=" + jobModelStr);
      zkClient.createPersistent(keyBuilder.getJobModelPath(jobModelVersion), jobModelStr);
      LOG.info("wrote jobModel path =" + keyBuilder.getJobModelPath(jobModelVersion));
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  /**
   * get the job model from ZK by version
   * @param jobModelVersion jobModel version to get
   * @return job model for this version
   */
  public JobModel getJobModel(String jobModelVersion) {
    LOG.info("pid=" + processorId + "read the model ver=" + jobModelVersion + " from " + keyBuilder.getJobModelPath(jobModelVersion));
    Object data = zkClient.readData(keyBuilder.getJobModelPath(jobModelVersion));
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

  public void publishNewJobModelVersion(String oldVersion, String newVersion) {
    Stat stat = new Stat();
    String currentVersion = zkClient.<String>readData(keyBuilder.getJobModelVersionPath(), stat);
    LOG.info("pid=" + processorId + " publishing new version: " + newVersion + "; oldVersion = " + oldVersion + "(" + stat.getVersion() + ")");
    if (currentVersion != null && !currentVersion.equals(oldVersion)) {
      throw new SamzaException("Someone change JMVersion while Leader was generating: expected" + oldVersion + ", got " + currentVersion);
    }
    int dataVersion = stat.getVersion();
    stat = zkClient.writeDataReturnStat(keyBuilder.getJobModelVersionPath(), newVersion, dataVersion);
    if (stat.getVersion() != dataVersion + 1)
      throw new SamzaException("Someone changed data version of the JMVersion while Leader was generating a new one. current= " + dataVersion + ", old version = " + stat.getVersion());

    LOG.info("pid=" + processorId +
             " published new version: " + newVersion + "; expected data version = " + dataVersion + "(" + stat.getVersion()
        +    ")");
  }


  /**
   * verify that given paths exist in ZK
   * @param paths zkPaths to verify
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
   * @param listener to be called in case of the change
   */
  public void subscribeToProcessorChange(IZkChildListener listener) {
    LOG.info("pid=" + processorId + " subscribing for child change at:" + keyBuilder.getProcessorsPath());
    zkClient.subscribeChildChanges(keyBuilder.getProcessorsPath(), listener);
  }

  public void deleteRoot() {
    String rootPath = keyBuilder.getRootPath();
    if (rootPath != null && !rootPath.isEmpty() && zkClient.exists(rootPath)) {
      LOG.info("pid=" + processorId + " Deleteing root: " + rootPath);
      zkClient.deleteRecursive(rootPath);
    }
  }
}
