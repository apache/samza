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

import java.util.concurrent.TimeUnit;

import java.util.concurrent.TimeoutException;
import org.apache.samza.coordinator.Latch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Latch of the sizeN is open when countDown() was called N times.
 * In this implementation a sequential node is created on every call of countDown().
 * When Nth node is created await() call returns.
 */
public class ZkProcessorLatch implements Latch {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkProcessorLatch.class);

  private final ZkUtils zkUtils;
  private final String participantId;
  private final String latchPath;
  private final String targetPath;

  public final static String LATCH_PATH = "latch";

  public ZkProcessorLatch(int size, String latchId, String participantId, ZkUtils zkUtils) {
    this.zkUtils = zkUtils;
    this.participantId = participantId;
    ZkKeyBuilder keyBuilder = this.zkUtils.getKeyBuilder();

    latchPath = String.format("%s/%s", keyBuilder.getRootPath(), LATCH_PATH + "_" + latchId);
    // TODO: Verify that validatePaths doesn't fail with exceptions
    zkUtils.validatePaths(new String[] {latchPath});
    targetPath =  String.format("%s/%010d", latchPath, size - 1);

    LOGGER.debug("ZkProcessorLatch targetPath " + targetPath);
  }

  @Override
  public void await(long timeout, TimeUnit timeUnit) throws TimeoutException {
    // waitUntilExists signals timeout by returning false as opposed to throwing exception. We internally need to map
    // the non-existence to a TimeoutException in order to respect the contract defined in Latch interface
    boolean targetPathExists = zkUtils.getZkClient().waitUntilExists(targetPath, timeUnit, timeout);

    if (!targetPathExists) {
      throw new TimeoutException("Timed out waiting for the targetPath");
    }
  }

  @Override
  public void countDown() {
    // create persistent (should be ephemeral? Probably not)
    String path = zkUtils.getZkClient().createPersistentSequential(latchPath + "/", participantId);
    LOGGER.debug("ZKProcessorLatch countDown created " + path);
  }
}
