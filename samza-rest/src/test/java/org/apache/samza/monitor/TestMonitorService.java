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
package org.apache.samza.monitor;

import org.apache.samza.config.MapConfig;
import org.apache.samza.monitor.mock.ExceptionThrowingMonitor;
import org.apache.samza.monitor.mock.InstantSchedulingProvider;
import org.apache.samza.rest.SamzaRestConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

public class TestMonitorService {

    @Test
    public void testGetMonitorsFromClassName() {
        // Test that monitors are instantiated properly from config strings.
        Monitor monitor = null;
        try {
            monitor = MonitorLoader.fromClassName("org.apache.samza.monitor.mock.DummyMonitor");
        } catch (InstantiationException e) {
            fail();
        }

        // Object should implement monitor().
        try {
            monitor.monitor();
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testMonitorExceptionIsolation() {
        // Test that an exception from a monitor doesn't bubble up out of the scheduler.
        Monitor monitor = new ExceptionThrowingMonitor();
        InstantSchedulingProvider provider = new InstantSchedulingProvider();

        // Initialize with a monitor that immediately throws an exception when run.
        Map<String, String> map = new HashMap<>();
        map.put(SamzaRestConfig.CONFIG_MONITOR_CLASSES, "org.apache.samza.monitor.mock.ExceptionThrowingMonitor");
        map.put(SamzaRestConfig.CONFIG_MONITOR_INTERVAL_MS, "1");
        SamzaRestConfig config = new SamzaRestConfig(new MapConfig(map));
        SamzaMonitorService monitorService = new SamzaMonitorService(config, provider);

        // This will throw if the exception isn't caught within the provider.
        monitorService.start();
        monitorService.stop();
    }

    @Test
    public void testScheduledExecutorSchedulingProvider() {
        // Test that the monitor is scheduled by the ScheduledExecutorSchedulingProvider
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        ScheduledExecutorSchedulingProvider provider =
                new ScheduledExecutorSchedulingProvider(executorService);

        // notifyingMonitor.monitor() should be called repeatedly.
        final CountDownLatch wasCalledLatch = new CountDownLatch(3);

        final Monitor notifyingMonitor = new Monitor() {
            @Override
            public void monitor() {
                wasCalledLatch.countDown();
            }
        };

        Runnable runnableMonitor = new Runnable() {
            public void run() {
                try {
                    notifyingMonitor.monitor();
                } catch (Exception e) {
                    // Must be caught because they are checked in monitor()
                    fail();
                }
            }
        };

        // monitor should get called every 1ms, so if await() misses the first call, there will be more.
        provider.schedule(runnableMonitor, 1);

        try {
            assertTrue(wasCalledLatch.await(5l, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executorService.shutdownNow();
        }

    }
}