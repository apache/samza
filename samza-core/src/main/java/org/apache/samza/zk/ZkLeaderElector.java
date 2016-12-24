package org.apache.samza.zk;

import java.util.Arrays;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.leaderelection.LeaderElector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;


public class ZkLeaderElector implements LeaderElector {
  public static final Logger log = LoggerFactory.getLogger(ZkLeaderElector.class);
  private final ZkUtils zkUtils;
  private final ZkListener zkListener;
  private final String processorIdStr;
  private final ZkKeyBuilder keyBuilder;

  private String leaderId = null;
  private final ZkLeaderListener zkLeaderListener = new ZkLeaderListener();
  private String currentSubscription = null;
  private final Random random = new Random();

  public ZkLeaderElector (String processorIdStr, ZkUtils zkUtils, ZkListener zkListener) {
    this.processorIdStr = processorIdStr;
    this.zkUtils = zkUtils;
    this.keyBuilder = this.zkUtils.getKeyBuilder();
    this.zkListener = zkListener;
  }

  @Override
  public boolean tryBecomeLeader() {
    String currentPath = zkUtils.getEphemeralPath();

    if (currentPath == null || currentPath.isEmpty()) {
      zkUtils.registerProcessorAndGetId();
      currentPath = zkUtils.getEphemeralPath();
    }

    List<String> children = zkUtils.getActiveProcessors();
    int index = children.indexOf(ZkKeyBuilder.parseIdFromPath(currentPath));

    if (index == -1) {
      // Retry register here??
      throw new SamzaException("Looks like we are no longer connected to Zk. Need to reconnect??");
    }

    if (index == 0) {
      log.info("pid=" + processorIdStr + " Eligible to be the leader!");
      leaderId = ZkKeyBuilder.parseIdFromPath(currentPath);
      return true;
    }

    log.info("pid=" + processorIdStr + ";index=" + index + ";children=" + Arrays.toString(children.toArray()) + " Not eligible to be a leader yet!");
    leaderId = ZkKeyBuilder.parseIdFromPath(children.get(0));
    String prevCandidate = children.get(index - 1);
    if (!prevCandidate.equals(currentSubscription)) {
      if (currentSubscription != null) {
        zkUtils.unsubscribeDataChanges(keyBuilder.getProcessorsPath() + "/" + currentSubscription, zkLeaderListener);
      }
      currentSubscription = prevCandidate;
      log.info("pid=" + processorIdStr + "Subscribing to " + prevCandidate);
      zkUtils.subscribeDataChanges(keyBuilder.getProcessorsPath() + "/" + currentSubscription, zkLeaderListener);
    }

    // Double check that the previous candidate still exists
    boolean prevCandidateExists = zkUtils.exists(keyBuilder.getProcessorsPath() + "/" + currentSubscription);
    if (prevCandidateExists) {
      log.info("pid=" + processorIdStr + "Previous candidate still exists. Continuing as non-leader");
    } else {
      // TODO - what actually happens here..
      try {
        Thread.sleep(random.nextInt(1000));
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
      log.info("pid=" + processorIdStr + "Previous candidate doesn't exist anymore. Trying to become leader again...");
      return tryBecomeLeader();
    }
    return false;
  }

  @Override
  public void resignLeadership() {
  }

  @Override
  public boolean amILeader() {
    return zkUtils.getEphemeralPath() != null
        && leaderId != null
        && leaderId.equals(ZkKeyBuilder.parseIdFromPath(zkUtils.getEphemeralPath()));
  }

  // Only by non-leaders
  class ZkLeaderListener implements IZkDataListener {

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      log.info("ZkLeaderListener::handleDataChange on path " + dataPath + " Data: " + data);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      log.info("ZkLeaderListener::handleDataDeleted on path " + dataPath);
      tryBecomeLeader();
    }
  }
}
