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
package org.apache.samza.container.host;

import junit.framework.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

public class TestStatisticsMonitorImpl {

  @Test
  public void testPhysicalMemoryReporting() throws Exception {
    final int numSamplesToCollect = 5;
    final CountDownLatch latch = new CountDownLatch(numSamplesToCollect);

    final StatisticsMonitorImpl monitor = new StatisticsMonitorImpl(10, new PosixCommandBasedStatisticsGetter());
    monitor.start();

    boolean result = monitor.registerListener(new SystemStatisticsMonitor.Listener() {

      @Override
      public void onUpdate(SystemMemoryStatistics sample) {
        // assert memory is greater than 10 bytes, as a sanity check
        Assert.assertTrue(sample.getPhysicalMemoryBytes() > 10);
        latch.countDown();
      }
    });

    if (!latch.await(5, TimeUnit.SECONDS)) {
      fail(String.format("Timed out waiting for listener to be give %d updates", numSamplesToCollect));
    }
    // assert that the registration for the listener was successful
    Assert.assertTrue(result);
    monitor.stop();

    // assert that attempting to register a listener after monitor stop results in failure of registration
    boolean registrationFailsAfterStop = monitor.registerListener(new SystemStatisticsMonitor.Listener() {
      @Override
      public void onUpdate(SystemMemoryStatistics sample) {
      }
    });
    Assert.assertFalse(registrationFailsAfterStop);
  }

  @Test
  public void testStopBehavior() throws Exception {

    final int numSamplesToCollect = 5;
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger numCallbacks = new AtomicInteger(0);

    final StatisticsMonitorImpl monitor = new StatisticsMonitorImpl(10, new PosixCommandBasedStatisticsGetter());

    monitor.start();
    monitor.registerListener(new SystemStatisticsMonitor.Listener() {

      @Override
      public void onUpdate(SystemMemoryStatistics sample) {
        Assert.assertTrue(sample.getPhysicalMemoryBytes() > 10);
        if (numCallbacks.incrementAndGet() == numSamplesToCollect) {
          //monitor.stop() is invoked from the same thread. So, there's no race between a stop() call and the
          //callback invocation for the next sample.
          monitor.stop();
          latch.countDown();
        }
      }
    });

    if (!latch.await(5, TimeUnit.SECONDS)) {
      fail(String.format("Timed out waiting for listener to be give %d updates", numSamplesToCollect));
    }
    // Ensure that we only receive as many callbacks
    Assert.assertEquals(numCallbacks.get(), numSamplesToCollect);
  }


}
