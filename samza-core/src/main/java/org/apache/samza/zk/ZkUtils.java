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
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistry;
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
 *  <b>Note on Session disconnect handling:</b>
 *  After the session has timed out, and restored we may still get some notifications from before (from the old
 *  session). To avoid this, we add a currentGeneration member, which starts with 0, and is increased each time
 *  a new session is established. Current value of this member is passed to each Listener when it is created.
 *  So if the Callback from this Listener comes with an old generation id - we ignore it.
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
  /* package private */static final String ZK_PROTOCOL_VERSION = "1.0";

  private final ZkClient zkClient;
  private volatile String ephemeralPath = null;
  private final ZkKeyBuilder keyBuilder;
  private final int connectionTimeoutMs;
  private final AtomicInteger currentGeneration;
  private final ZkUtilsMetrics metrics;

  public void incGeneration() {
    currentGeneration.incrementAndGet();
  }

  public int getGeneration() {
    return currentGeneration.get();
  }

  public ZkUtils(ZkKeyBuilder zkKeyBuilder, ZkClient zkClient, int connectionTimeoutMs, MetricsRegistry metricsRegistry) {
    this.keyBuilder = zkKeyBuilder;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.zkClient = zkClient;
    this.metrics = new ZkUtilsMetrics(metricsRegistry);
    this.currentGeneration = new AtomicInteger(0);
  }

  public void connect() throws ZkInterruptedException {
    boolean isConnected = zkClient.waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS);
    if (!isConnected) {
      metrics.zkConnectionError.inc();
      throw new RuntimeException("Unable to connect to Zookeeper within connectionTimeout " + connectionTimeoutMs + "ms. Shutting down!");
    }
  }

  // reset all zk-session specific state
  public void unregister() {
    ephemeralPath = null;
  }

  public ZkClient getZkClient() {
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
    String processorId = data.getProcessorId();
    if (ephemeralPath == null) {
      ephemeralPath = zkClient.createEphemeralSequential(keyBuilder.getProcessorsPath() + "/", data.toString());
      LOG.info("Created ephemeral path: {} for processor: {} in zookeeper.", ephemeralPath, data);
      ProcessorNode processorNode = new ProcessorNode(data, ephemeralPath);
      // Determine if there are duplicate processors with this.processorId after registration.
      if (!isValidRegisteredProcessor(processorNode)) {
        LOG.info("Processor: {} is duplicate. Deleting zookeeper node at path: {}.", processorId, ephemeralPath);
        zkClient.delete(ephemeralPath);
        metrics.deletions.inc();
        throw new SamzaException(String.format("Processor: %s is duplicate in the group. Registration failed.", processorId));
      }
    } else {
      LOG.info("Ephemeral path: {} exists for processor: {} in zookeeper.", ephemeralPath, data);
    }
    return ephemeralPath;
  }

  /**
   * Determines the validity of processor registered with zookeeper.
   *
   * If there are multiple processors registered with same processorId,
   * the processor with lexicographically smallest zookeeperPath is considered valid
   * and all the remaining processors are invalid.
   *
   * Two processors will not have smallest zookeeperPath because of sequentialId guarantees
   * of zookeeper for ephemeral nodes.
   *
   * @param processor to check for validity condition in processors group.
   * @return true if the processor is valid. false otherwise.
   */
  private boolean isValidRegisteredProcessor(final ProcessorNode processor) {
    String processorId = processor.getProcessorData().getProcessorId();
    List<ProcessorNode> processorNodes = getAllProcessorNodes().stream()
                                                               .filter(processorNode -> processorNode.processorData.getProcessorId().equals(processorId))
                                                               .collect(Collectors.toList());
    // Check for duplicate processor condition(if more than one processor exist for this processorId).
    if (processorNodes.size() > 1) {
      // There exists more than processor for provided `processorId`.
      LOG.debug("Processor nodes in zookeeper: {} for processorId: {}.", processorNodes, processorId);
      // Get all ephemeral processor paths
      TreeSet<String> sortedProcessorPaths = processorNodes.stream()
                                                           .map(ProcessorNode::getEphemeralPath)
                                                           .collect(Collectors.toCollection(TreeSet::new));
      // Check if smallest path is equal to this processor's ephemeralPath.
      return sortedProcessorPaths.first().equals(processor.getEphemeralPath());
    }
    // There're no duplicate processors. This is a valid registered processor.
    return true;
  }

  /**
   * Fetches all the ephemeral processor nodes of a standalone job from zookeeper.
   * @return a list of {@link ProcessorNode}, where each ProcessorNode represents a registered stream processor.
   */
  List<ProcessorNode> getAllProcessorNodes() {
    List<String> processorZNodes = getSortedActiveProcessorsZnodes();
    LOG.debug("Active ProcessorZNodes in zookeeper: {}.", processorZNodes);
    List<ProcessorNode> processorNodes = new ArrayList<>();
    for (String processorZNode: processorZNodes) {
      String ephemeralProcessorPath = String.format("%s/%s", keyBuilder.getProcessorsPath(), processorZNode);
      String data = readProcessorData(ephemeralProcessorPath);
      if (data != null) {
        processorNodes.add(new ProcessorNode(new ProcessorData(data), ephemeralProcessorPath));
      }
    }
    return processorNodes;
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
  private String readProcessorData(String fullPath) {
    try {
      String data = zkClient.readData(fullPath, true);
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
        String processorData = readProcessorData(fullPath);
        if (processorData != null) {
          processorIds.add(new ProcessorData(processorData).getProcessorId());
        }
      }
      Collections.sort(processorIds);
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
    try {
      zkClient.close();
    } catch (ZkInterruptedException e) {
      LOG.warn("Interrupted when closing zkClient. Clearing the interrupted status and retrying.", e);
      Thread.interrupted();
      zkClient.close();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Generation enforcing zk listener abstract class.
   * It helps listeners, which extend it, to notAValidEvent old generation events.
   * We cannot use 'sessionId' for this because it is not available through ZkClient (at leaste without reflection)
   */
  public abstract static class GenIZkChildListener implements IZkChildListener {
    private final int generation;
    private final ZkUtils zkUtils;
    private final String listenerName;

    public GenIZkChildListener(ZkUtils zkUtils, String listenerName) {
      generation = zkUtils.getGeneration();
      this.zkUtils = zkUtils;
      this.listenerName = listenerName;
    }

    protected boolean notAValidEvent() {
      int curGeneration = zkUtils.getGeneration();
      if (curGeneration != generation) {
        LOG.warn("SKIPPING handleDataChanged for " + listenerName +
            " from wrong generation. current generation=" + curGeneration + "; callback generation= " + generation);
        return true;
      }
      return false;
    }
  }

  public abstract static class GenIZkDataListener implements IZkDataListener {
    private final int generation;
    private final ZkUtils zkUtils;
    private final String listenerName;

    public GenIZkDataListener(ZkUtils zkUtils, String listenerName) {
      generation = zkUtils.getGeneration();
      this.zkUtils = zkUtils;
      this.listenerName = listenerName;
    }

    protected boolean notAValidEvent() {
      int curGeneration = zkUtils.getGeneration();
      if (curGeneration != generation) {
        LOG.warn("SKIPPING handleDataChanged for " + listenerName +
            " from wrong generation. curGen=" + curGeneration + "; cb gen= " + generation);
        return true;
      }
      return false;
    }

  }

  /**
    * subscribe for changes of JobModel version
    * @param dataListener describe this
    */
  public void subscribeToJobModelVersionChange(GenIZkDataListener dataListener) {
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
    LOG.info("Read the model ver=" + jobModelVersion + " from " + keyBuilder.getJobModelPath(jobModelVersion));
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
    String jobModelVersion = zkClient.readData(keyBuilder.getJobModelVersionPath(), true);
    metrics.reads.inc();
    return jobModelVersion;
  }

  /**
   * Generates the next JobModel version that should be used by a processor group in a rebalancing phase
   * for coordination.
   * @param currentJobModelVersion the current version of JobModel.
   * @return the next JobModel version.
   */
  public String getNextJobModelVersion(String currentJobModelVersion) {
    if (currentJobModelVersion == null) {
      return  "1";
    } else {
      /**
       * There's inconsistency between the maximum published jobModel version and value stored in jobModelVersion
       * zookeeper node. Short term fix is to read all published jobModel versions and choose the maximum version. If there's a
       * inconsistency, update the jobModelVersionPath with maximum published jobModelVersion.
       */
      List<String> publishedJobModelVersions = zkClient.getChildren(keyBuilder.getJobModelPathPrefix());
      metrics.reads.inc(publishedJobModelVersions.size());
      String maxPublishedJMVersion = publishedJobModelVersions.stream()
                                                              .max(Comparator.comparingInt(Integer::valueOf)).orElse("0");
      return Integer.toString(Math.max(Integer.valueOf(currentJobModelVersion), Integer.valueOf(maxPublishedJMVersion)) + 1);
    }
  }

  /**
   * publish the version number of the next JobModel
   * @param oldVersion - used to validate, that no one has changed the version in the meanwhile.
   * @param newVersion - new version.
   */
  public void publishJobModelVersion(String oldVersion, String newVersion) {
    Stat stat = new Stat();
    String currentVersion = zkClient.readData(keyBuilder.getJobModelVersionPath(), stat);
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

  // validate that Zk protocol currently used by the job is the same as in this participant
  public void validateZkVersion() {

    // Version of the protocol is written into root znode. If root does not exist yet we need to create one.
    String rootPath = keyBuilder.getRootPath();
    if (!zkClient.exists(rootPath)) {
      try {
        // attempt to create the root with the correct version
        zkClient.createPersistent(rootPath, ZK_PROTOCOL_VERSION);
        LOG.info("Created zk root node: " + rootPath + " with zk version " + ZK_PROTOCOL_VERSION);
        return;
      } catch (ZkNodeExistsException e) {
        // ignoring
        LOG.warn("root path " + rootPath + " already exists.");
      }
    }
    // if exists, verify the version
    Stat stat = new Stat();
    String version = zkClient.readData(rootPath, stat);
    if (version == null) {
      // for backward compatibility, if no value - assume 1.0
      try {
        zkClient.writeData(rootPath, "1.0", stat.getVersion());
      } catch (ZkBadVersionException e) {
        // if the write failed with ZkBadVersionException it means someone else already wrote a version, so we can ignore it.
      }
      // re-read the updated version
      version = zkClient.readData(rootPath);
    }
    LOG.info("Current version for zk root node: " + rootPath + " is " + version + ", expected version is " + ZK_PROTOCOL_VERSION);
    if (!version.equals(ZK_PROTOCOL_VERSION)) {
      throw new SamzaException("ZK Protocol mismatch. Expected " + ZK_PROTOCOL_VERSION + "; found " + version);
    }
  }

  /**
   * verify that given paths exist in ZK
   * @param paths - paths to verify or create
   */
  public void validatePaths(String[] paths) {
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
    LOG.info("Subscribing for child change at:" + keyBuilder.getProcessorsPath());
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

  void deleteOldJobModels(int numVersionsToLeave) {
    // read current list of JMs
    String path = keyBuilder.getJobModelPathPrefix();
    LOG.info("About to delete jm path=" + path);
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
    LOG.info("About to delete old barrier paths from " + path);
    List<String> znodeIds = zkClient.getChildren(path);
    LOG.info("List of all zkNodes: " + znodeIds);
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
      return;
    }
    if (zNodeIds.size() > numVersionsToLeave) {
      Collections.sort(zNodeIds, c);
      // get the znodes to delete
      int size = zNodeIds.size();
      List<String> zNodesToDelete = zNodeIds.subList(0, zNodeIds.size() - numVersionsToLeave);
      LOG.info("Starting cleanup of barrier version zkNodes. From size=" + size + " to size " + zNodesToDelete.size() + "; numberToLeave=" + numVersionsToLeave);
      for (String znodeId : zNodesToDelete) {
        String pathToDelete = path + "/" + znodeId;
        try {
          LOG.info("deleting " + pathToDelete);
          zkClient.deleteRecursive(pathToDelete);
          metrics.deletions.inc();
        } catch (Exception e) {
          LOG.warn("delete of node " + pathToDelete + " failed.", e);
        }
      }
    }
  }
  /**
   * Represents zookeeper processor node.
   */
  static class ProcessorNode {
    private final ProcessorData processorData;

    // Ex: /test/processors/0000000000
    private final String ephemeralProcessorPath;

    ProcessorNode(ProcessorData processorData, String ephemeralProcessorPath) {
      this.processorData = processorData;
      this.ephemeralProcessorPath = ephemeralProcessorPath;
    }

    ProcessorData getProcessorData() {
      return processorData;
    }

    String getEphemeralPath() {
      return ephemeralProcessorPath;
    }

    @Override
    public String toString() {
      return String.format("[ProcessorData: %s, ephemeralProcessorPath: %s]", processorData, ephemeralProcessorPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(processorData, ephemeralProcessorPath);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      final ProcessorNode other = (ProcessorNode) obj;
      return Objects.equals(processorData, other.processorData) && Objects.equals(ephemeralProcessorPath, other.ephemeralProcessorPath);
    }
  }
}
