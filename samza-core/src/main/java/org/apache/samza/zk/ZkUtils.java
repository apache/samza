package org.apache.samza.zk;

import java.io.IOException;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class ZkUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ZkUtils.class);

  public final ReentrantLock lock = new ReentrantLock();

  private final ZkStateChangeHandler zkStateChangeHandler;
  private final ZkClient zkClient;
  private final ZkConnection zkConnnection;
  private volatile String ephemeralPath = null;
  private final ZkKeyBuilder keyBuilder;
  private final int sessionTimeoutMs;
  private final int connectionTimeoutMs;
  private final ScheduleAfterDebounceTime debounceTimer;
  private final String processorId;

  public ZkUtils(String zkConnectString, ScheduleAfterDebounceTime debounceTimer, String processorId) {
    this(new ZkKeyBuilder(), zkConnectString, debounceTimer, processorId, ZkConfig.DEFAULT_SESSION_TIMEOUT_MS, ZkConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  public ZkUtils(ZkKeyBuilder zkKeyBuilder, String zkConnectString, ScheduleAfterDebounceTime debounceTimer, String processorId, int sessionTimeoutMs, int connectionTimeoutMs) {
    this.keyBuilder = zkKeyBuilder;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.zkConnnection = new ZkConnection(zkConnectString, this.sessionTimeoutMs);
    this.zkClient = new ZkClient(zkConnnection, this.connectionTimeoutMs);
    this.zkClient.waitForKeeperState(Watcher.Event.KeeperState.SyncConnected, 10000, TimeUnit.MILLISECONDS);
    this.debounceTimer = debounceTimer;
    this.zkStateChangeHandler = new ZkStateChangeHandler(debounceTimer);
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
    for(String path: paths) {
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

  ////////////////////////// TEMP ///////////////// NEEDS to be discussed ////////////////
  public void publishNewJobModel(String jobModelVersion, JobModel jobModel) {
    try {
      // We assume (needs to be verified) that this call will FAIL if the node already exists!!!!!!!!
      ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
      String jobModelStr = mmapper.writeValueAsString(jobModel);
      LOG.info("pid=" + processorId + " jobModelAsString=" + jobModelStr);
      zkClient.createPersistent(keyBuilder.getJobModelPath(jobModelVersion), jobModelStr);
      LOG.info("wrote jobModel path =" + keyBuilder.getJobModelPath(jobModelVersion));
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }
  public JobModel getJobModel(String jobModelVersion) {
    LOG.info("pid=" + processorId + "read the model ver=" + jobModelVersion + " from " + keyBuilder.getJobModelPath(jobModelVersion));
    Object data = zkClient.readData(keyBuilder.getJobModelPath(jobModelVersion));
    ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
    JobModel jm;
    try {
      jm = mmapper.readValue((String)data, JobModel.class);
    } catch (IOException e) {
      throw new SamzaException("failed to read JobModel from ZK", e);
    }
    return jm;
  }
  ///////////////////////////////////////////////////////////////////////////


  public String getJobModelVersion() {
    return zkClient.<String>readData(keyBuilder.getJobModelVersionPath());
  }

  public void publishNewJobModelVersion(String oldVersion, String newVersion) {
    Stat stat = new Stat();
    String currentVersion =  zkClient.<String>readData(keyBuilder.getJobModelVersionPath(), stat);
    LOG.info("pid=" + processorId + " publishing new version: " + newVersion + "; oldVersion = " + oldVersion + "(" + stat.getVersion() + ")");
    if(currentVersion != null && !currentVersion.equals(oldVersion)) {
      throw new SamzaException("Someone change JMVersion while Leader was generating: expected" + oldVersion  + ", got " + currentVersion);
    }
    int dataVersion = stat.getVersion();
    stat = zkClient.writeDataReturnStat(keyBuilder.getJobModelVersionPath(), newVersion, dataVersion);
    if(stat.getVersion() != dataVersion + 1)
      throw new SamzaException("Someone changed data version of the JMVersion while Leader was generating a new one. current= " + dataVersion + ", old version = " + stat.getVersion());

    LOG.info("pid=" + processorId +
        " published new version: " + newVersion + "; expected dataVersion = " + dataVersion + "(" + stat.getVersion()
            + ")");
  }

  /**
   * subscribe for changes of JobModel version
   * @param dataListener describe this
   */
  public void subscribeToJobModelVersionChange(IZkDataListener dataListener) {
    LOG.info("pid=" + processorId + " subscribing for jm version change at:" + keyBuilder.getJobModelVersionPath());
    zkClient.subscribeDataChanges(keyBuilder.getJobModelVersionPath(), dataListener);
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
    zkClient.close();
  }

  public void deleteRoot() {
    String rootPath = keyBuilder.getRootPath();
    if(rootPath != null && !rootPath.isEmpty() && zkClient.exists(rootPath)) {
      LOG.info("pid=" + processorId + " Deleteing root: " + rootPath);
      zkClient.deleteRecursive(rootPath);
    }
  }

  class ZkStateChangeHandler implements IZkStateListener {
    private final ScheduleAfterDebounceTime debounceTimer;
    public ZkStateChangeHandler(ScheduleAfterDebounceTime debounceTimer) {
      this.debounceTimer = debounceTimer;
    }

    /**
     * Called when the zookeeper connection state has changed.
     *
     * @param state The new state.
     * @throws Exception On any error.
     */
    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {    }

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
