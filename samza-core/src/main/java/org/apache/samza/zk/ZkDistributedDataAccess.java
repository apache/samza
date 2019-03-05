package org.apache.samza.zk;

import org.apache.samza.coordinator.DistributedDataAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkDistributedDataAccess implements DistributedDataAccess {
  public static final Logger LOG = LoggerFactory.getLogger(ZkDistributedDataAccess.class);

  private final ZkUtils zkUtils;
  private final ZkKeyBuilder keyBuilder;

  public ZkDistributedDataAccess(ZkUtils zkUtils) {
    if (zkUtils == null) {
      throw new RuntimeException("Cannot operate ZkDistributedDataAccess without ZkUtils.");
    }
    this.zkUtils = zkUtils;
    this.keyBuilder = zkUtils.getKeyBuilder();
  }

  public Object readData(String key) {
    String absolutePath = keyBuilder.getRootPath() + "/" + key;
    if(!zkUtils.getZkClient().exists(absolutePath)) {
      return null;
    }

    return zkUtils.getZkClient().readData(absolutePath);
  }

  public void writeData(String key, Object data) {
    String absolutePath = keyBuilder.getRootPath() + "/" + key;
    zkUtils.validatePaths(new String[]{absolutePath});
    zkUtils.writeData(absolutePath, data);
  }
}
