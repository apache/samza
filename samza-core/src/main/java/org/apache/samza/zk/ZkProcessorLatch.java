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
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.ProcessorLatch;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkProcessorLatch implements ProcessorLatch {
  public static final Logger LOG = LoggerFactory.getLogger(ZkProcessorLatch.class);

  private final ZkConfig zkConfig;
  private final ZkUtils zkUtils;
  private final String processorIdStr;
  private final ZkKeyBuilder keyBuilder;
  private final String latchId;

  private final String latchPath;
  private final String countPath;

  //private final String targetPath;
  
  private boolean done = false;
  private Thread currentThread = null;

  private final static String LATCH_PATH = "latch";
  private final int size; // latch size

  public ZkProcessorLatch(int size, String latchId, String processorId, ZkConfig zkConfig, ZkUtils zkUtils) {
    this.zkConfig = zkConfig;
    this.zkUtils = zkUtils;
    this.processorIdStr = processorId;
    this.latchId = latchId;
    this.keyBuilder = this.zkUtils.getKeyBuilder();
    this.size = size;

    if(size <= 0)
      throw new SamzaException("Latch size cannot be 0");

    latchPath = String.format("%s/%s", keyBuilder.getRootPath(), LATCH_PATH);
    countPath = String.format("%s/%s", latchPath, "count");
    zkUtils.makeSurePersistentPathsExists(new String[] {latchPath, countPath});
    //targetPath =  String.format("%s/%d", latchPath, 0);
    System.out.println("countPath " + countPath);
    zkUtils.getZkClient().writeData(countPath, String.valueOf(size));
  }

  @Override
  public void await(TimeUnit tu, long timeout) throws TimeoutException {
    //zkUtils.getZkClient().waitUntilExists(targetPath, TimeUnit.MILLISECONDS, timeout);
    currentThread = Thread.currentThread();

    zkUtils.getZkClient().subscribeDataChanges(countPath, new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data)
          throws Exception {
        String countStr = zkUtils.getZkClient().readData(countPath);
        System.out.println("read count = " + countStr);
        if(Long.valueOf(countStr) <= 0) { // done
          System.out.println("count is ready");
          done = true;
          currentThread.interrupt();
        }
      }

      @Override
      public void handleDataDeleted(String dataPath)
          throws Exception {
        throw new SamzaException("latch count pat = " + dataPath + " got deleted");
      }
    });

    // check if the count is not 0 now
    String countStr = zkUtils.getZkClient().readData(countPath);
    System.out.println("read 1st count = " + countStr);
    if(Long.valueOf(countStr) <= 0) { // done
      System.out.println("1st count is ready");
      done = true;
    }

    while(!done) {
      try {
        Thread.sleep(timeout);
      } catch(InterruptedException e) {
        System.out.println("Wait interrupted. done=" + done);
        // expected path
        if(done)
          return;
        // else go back to sleep
        continue;
      }
      // if we reached here it means we timed out
      throw new TimeoutException("latch timed out after " + timeout + " ms.");
    }
  }

  @Override
  public void countDown() {
    // create persistent (should be ephemeral? Probably not)
    //String path = zkUtils.getZkClient().createPersistentSequential(latchPath + "/", processorIdStr);
    //System.out.println("countDown created " + path);
    while(true) {
      Stat stat = new Stat();
      String countStr = zkUtils.getZkClient().readData(countPath, stat);
      Long count = Long.valueOf(countStr);
      if (count <= 0) return; // latch done

      String newValue = String.valueOf(--count);
      System.out.println("writing new value " + newValue);
      try {
        zkUtils.getZkClient().writeData(countPath, newValue, stat.getVersion());
        return; // successfully updated
      } catch (Exception e) {
        LOG.warn("write failed because of version. Someone else had updated the counter. Will retry. newCount = " + newValue, e);
      }
    }
  }
}
