package org.apache.samza.zk;

import org.apache.samza.coordinator.DistributedDataAccess;
import org.apache.samza.coordinator.DistributedDataWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkDistributedDataAccess implements DistributedDataAccess {
  public static final Logger LOG = LoggerFactory.getLogger(ZkDistributedDataAccess.class);
  private final ZkUtils zkUtils;
  private final ZkKeyBuilder keyBuilder;
  private DistributedDataWatcher watcher;

  public ZkDistributedDataAccess(ZkUtils zkUtils) {
    if (zkUtils == null) {
      throw new RuntimeException("Cannot operate ZkDistributedDataAccess without ZkUtils.");
    }
    this.zkUtils = zkUtils;
    this.keyBuilder = zkUtils.getKeyBuilder();
  }

  public Object readData(String key, DistributedDataWatcher watcher) {
    this.watcher = watcher;
    String absolutePath = keyBuilder.getRootPath() + "/" + key;
    zkUtils.getZkClient().subscribeDataChanges(absolutePath, new ZkDistributedDataChangeHandler(zkUtils));
    if(!zkUtils.getZkClient().exists(absolutePath)) {
      return null;
    }
    return zkUtils.getZkClient().readData(absolutePath);
  }

  public void writeData(String key, Object data, DistributedDataWatcher watcher) {
    this.watcher = watcher;
    String absolutePath = keyBuilder.getRootPath() + "/" + key;
    zkUtils.getZkClient().subscribeDataChanges(absolutePath, new ZkDistributedDataChangeHandler(zkUtils));
    zkUtils.validatePaths(new String[]{absolutePath});
    zkUtils.writeData(absolutePath, data);
    return;
  }

  class ZkDistributedDataChangeHandler extends ZkUtils.GenerationAwareZkDataListener {

    public ZkDistributedDataChangeHandler(ZkUtils zkUtils) {
      super(zkUtils, "ZkDistributedDataChangeHandler");
    }

    /**
     * Invoked when there is a change to the  data z-node.
     */
    @Override
    public void doHandleDataChange(String dataPath, Object data) {
      watcher.handleDataChange(data);
    }

    @Override
    public void doHandleDataDeleted(String dataPath) {
      watcher.handleDataDeleted();
    }
  }
}
