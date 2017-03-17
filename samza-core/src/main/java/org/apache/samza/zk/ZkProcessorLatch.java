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
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.ProcessorLatch;


public class ZkProcessorLatch implements ProcessorLatch {

  public final ZkConfig zkConfig;
  public final ZkUtils zkUtils;
  public final String processorIdStr;
  public final ZkKeyBuilder keyBuilder;
  public final String latchId;

  public final String latchPath;
  public final String targetPath;

  public final static String LATCH_PATH = "latch";
  public final int size; // latch size

  public ZkProcessorLatch(int size, String latchId, String processorId, ZkConfig zkConfig, ZkUtils zkUtils) {
    this.zkConfig = zkConfig;
    this.zkUtils = zkUtils;
    this.processorIdStr = processorId;
    this.latchId = latchId;
    this.keyBuilder = this.zkUtils.getKeyBuilder();
    this.size = size;

    latchPath = String.format("%s/%s", keyBuilder.getRootPath(), LATCH_PATH + "_" + latchId);
    zkUtils.makeSurePersistentPathsExists(new String[] {latchPath});
    targetPath =  String.format("%s/%010d", latchPath, size - 1);
    System.out.println("targetPath " + targetPath);
  }

  @Override
  public void await(TimeUnit tu, long timeout) {
    zkUtils.getZkClient().waitUntilExists(targetPath, TimeUnit.MILLISECONDS, timeout);
  }

  @Override
  public void countDown() {
    // create persistent (should be ephemeral? Probably not)
    String path = zkUtils.getZkClient().createPersistentSequential(latchPath + "/", processorIdStr);
    System.out.println("countDown created " + path);
  }
}
