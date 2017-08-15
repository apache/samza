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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.CoordinationLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkCoordinationLock implements CoordinationLock {

  private final static Logger LOG = LoggerFactory.getLogger(ZkCoordinationLock.class);
  private final String participantId;
  private final ZkUtils zkUtils;
  private final String lockPath;
  private String nodePath;

  public ZkCoordinationLock(String groupId, String participantId, ZkUtils zkUtils) {
    this.participantId = participantId;
    if(StringUtils.isEmpty(groupId))
      throw new SamzaException("groupId cannot be empty.");
    this.zkUtils = zkUtils;

    lockPath = zkUtils.getKeyBuilder().getRootPath() + "/coordinationLock";
    zkUtils.makeSurePersistentPathsExists(new String [] {lockPath});
    System.out.println("Created lock object in " + lockPath);
  }


  private static class LockException extends RuntimeException { }

  @Override
  public void lock(TimeUnit tu, long timeout) throws TimeoutException {

    nodePath = zkUtils.getZkClient().createEphemeralSequential(lockPath + "/", participantId);

    System.out.println("registered");

    List<String> children = zkUtils.getZkClient().getChildren(lockPath);
    Collections.sort(children); // TODO verify that sorting works

    System.out.println("children: " + children);

    String znode = ZkKeyBuilder.parseIdFromPath(nodePath);
    int index = children.indexOf(znode);
    if (children.size() == 0 || index == -1) {
      throw new SamzaException("Error happened while try to acquire a lock");
    }

    System.out.println("participantid = " + participantId + ";znode = " + znode + ";index = " + index);
    if (index == 0) {
      LOG.info("Acquired lock for participant id: {}", participantId);
      return;
    } else {
      final CountDownLatch cl = new CountDownLatch(1);
      String predecessor = children.get(index - 1);
      String predecessorPath = lockPath + "/" + predecessor;
      System.out.println("node " + nodePath + " watches " + predecessor);
      zkUtils.subscribeDataChanges(predecessorPath, new IZkDataListener() {
        @Override
        public void handleDataChange(String dataPath, Object data)
            throws Exception {
          // nothing
        }

        @Override
        public void handleDataDeleted(String dataPath)
            throws Exception {
          System.out.println("got delete for " + dataPath );
          cl.countDown();
        }
      });

      if (! zkUtils.exists(predecessorPath) && cl.getCount() != 0) {
        // was deleted before we subscribed, try again.
        System.out.println("SOMETHING CHANGED:" + predecessorPath + " disappeared for " + nodePath);
        cl.countDown();
        return;
      }

      try {
        if (!cl.await(timeout, tu)) {
          throw new TimeoutException("Couldn't acquire lock in the allocated time");
        }
      } catch (InterruptedException e) {
        throw new TimeoutException("Couldn't acquire lock in the allocated time." + e.getLocalizedMessage());
      }
    }
  }

  @Override
  public void unlock() {
    if (nodePath != null) {
      boolean status = zkUtils.getZkClient().delete(nodePath);
      System.out.println("delete status = " + status);
    }
  }

  @Override
  public void close() {
    if (zkUtils != null)
      zkUtils.close();
  }
}
