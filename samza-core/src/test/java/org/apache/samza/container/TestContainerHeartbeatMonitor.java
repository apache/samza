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

package org.apache.samza.container;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class TestContainerHeartbeatMonitor {

  @Test
  public void testCallbackWhenHeartbeatDead()
      throws InterruptedException {
    ContainerHeartbeatClient mockClient = mock(ContainerHeartbeatClient.class);
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Runnable onExpired = () -> {
      countDownLatch.countDown();
    };
    ContainerHeartbeatMonitor monitor = new ContainerHeartbeatMonitor(onExpired, mockClient);
    when(mockClient.isAlive()).thenReturn(false);
    monitor.start();
    boolean success = countDownLatch.await(2, TimeUnit.SECONDS);
    Assert.assertTrue(success);
  }

  @Test
  public void testDoesNotCallbackWhenHeartbeatAlive()
      throws InterruptedException {
    ContainerHeartbeatClient client = mock(ContainerHeartbeatClient.class);
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Runnable onExpired = () -> {
      countDownLatch.countDown();
    };
    ContainerHeartbeatMonitor monitor = new ContainerHeartbeatMonitor(onExpired, client);
    when(client.isAlive()).thenReturn(true);
    monitor.start();
    boolean success = countDownLatch.await(2, TimeUnit.SECONDS);
    Assert.assertFalse(success);
    Assert.assertEquals(1, countDownLatch.getCount());
  }
}
