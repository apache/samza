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

import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.LeaderElector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ZkControllerImpl implements ZkController {
  private static final Logger LOG = LoggerFactory.getLogger(ZkControllerImpl.class);

  private final String processorIdStr;
  private final ZkUtils zkUtils;
  private final ZkControllerListener zkControllerListener;
  private final LeaderElector zkLeaderElector;

  public ZkControllerImpl(String processorIdStr, ZkUtils zkUtils,
      ZkControllerListener zkControllerListener, LeaderElector zkLeaderElector) {
    this.processorIdStr = processorIdStr;
    this.zkUtils = zkUtils;
    this.zkControllerListener = zkControllerListener;
    this.zkLeaderElector = zkLeaderElector;

    init();
  }

  private void init() {
    ZkKeyBuilder keyBuilder = zkUtils.getKeyBuilder();

    zkUtils.validatePaths(new String[]{keyBuilder.getProcessorsPath(), keyBuilder.getJobModelVersionPath(), keyBuilder
            .getJobModelPathPrefix()});
  }

  @Override
  public void register() {
    // TODO - make a loop here with some number of attempts.
    // possibly split into two method - becomeLeader() and becomeParticipant()
    zkLeaderElector.tryBecomeLeader();

    // make sure we are connection to a job that uses the same ZK communication protocol version.
    try {
      zkUtils.validateZkVersion();
    } catch (SamzaException e) {
      // IMPORTANT: Mismatch of the version, means we are trying to join a job, started by processors with different version.
      // If there are no processors running, this is the place to do the migration to the new
      // ZK structure.
      // If some processors are running, then this processor should fail with an error to tell the user to stop all
      // the processors before upgrading to this new version.
      // TODO migration here
      // for now we just rethrow the exception
      throw e;
    }


    // subscribe to JobModel version updates
    zkUtils.subscribeToJobModelVersionChange(new ZkJobModelVersionChangeHandler(zkUtils));
  }

  @Override
  public boolean isLeader() {
    return zkLeaderElector.amILeader();
  }

  @Override
  public void stop() {
    if (isLeader()) {
      zkLeaderElector.resignLeadership();
    }

    // close zk connection
    if (zkUtils != null) {
      zkUtils.getZkClient().close();
    }
  }

  @Override
  public void subscribeToProcessorChange() {
    zkUtils.subscribeToProcessorChange(new ProcessorChangeHandler(zkUtils));
  }

  // Only by Leader
  class ProcessorChangeHandler extends ZkUtils.GenIZkChildListener {

    public ProcessorChangeHandler(ZkUtils zkUtils) {
      super(zkUtils, "ProcessorChangeHandler");
    }

    /**
     * Called when the children of the given path changed.
     *
     * @param parentPath    The parent path
     * @param currentChildren The children or null if the root node (parent path) was deleted.
     * @throws Exception
     */
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren)
        throws Exception {
      if (notAValidEvent())
        return;

      if (currentChildren == null) {
        // this may happen only in case of exception in ZK. It happens if the zkNode has been deleted.
        // So the notification will pass 'null' as the list of children. Exception should be visible in the logs.
        // It makes no sense to pass it further down.
        LOG.error("handleChildChange on path " + parentPath + " was invoked with NULL list of children");
        return;
      }
      LOG.info(
          "ZkControllerImpl::ProcessorChangeHandler::handleChildChange - Path: " + parentPath +
              "  Current Children: " + currentChildren);
      zkControllerListener.onProcessorChange(currentChildren);

    }
  }

  class ZkJobModelVersionChangeHandler extends ZkUtils.GenIZkDataListener {

    public ZkJobModelVersionChangeHandler(ZkUtils zkUtils) {
      super(zkUtils, "ZkJobModelVersionChangeHandler");
    }
    /**
     * Called when there is a change to the data in JobModel version path
     * To the subscribers, it signifies that a new version of JobModel is available.
     */
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      if (notAValidEvent())
        return;

      LOG.info("pid=" + processorIdStr + ". Got notification on version update change. path=" + dataPath + "; data="
          + data);
      zkControllerListener.onNewJobModelAvailable((String) data);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      if (notAValidEvent())
        return;

      throw new SamzaException("version update path has been deleted!");
    }
  }
}
