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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.monitor.mock.DummyMonitorFactory;
import org.apache.samza.monitor.mock.ExceptionThrowingMonitorFactory;
import org.apache.samza.monitor.mock.InstantSchedulingProvider;
import org.apache.samza.monitor.mock.MockMonitorFactory;
import org.apache.samza.rest.SamzaRestConfig;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;

import static junit.framework.TestCase.assertTrue;
import static org.apache.samza.monitor.MonitorConfig.CONFIG_MONITOR_FACTORY_CLASS;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestMonitorService {

    private static final MetricsRegistry METRICS_REGISTRY = new NoOpMetricsRegistry();

    @Test
    public void testMonitorsShouldBeInstantiatedProperly() {
        // Test that a monitor should be instantiated properly by invoking
        // the appropriate factory method.
        Map<String, String> configMap = ImmutableMap.of(CONFIG_MONITOR_FACTORY_CLASS,
                                                        DummyMonitorFactory.class.getCanonicalName());
        Monitor monitor = null;
        try {
            monitor = MonitorLoader.instantiateMonitor(new MonitorConfig(new MapConfig(configMap)),
                                                       METRICS_REGISTRY);
        } catch (InstantiationException e) {
            fail();
        }
        assertNotNull(monitor);
        // Object should implement the monitor().
        try {
            monitor.monitor();
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testShouldGroupRelevantMonitorConfigTogether() {
        // Test that Monitor Loader groups relevant config together.
        Map<String, String> firstMonitorConfig = ImmutableMap.of("monitor.monitor1.factory.class",
                                                                 "org.apache.samza.monitor.DummyMonitor",
                                                                 "monitor.monitor1.scheduling.interval.ms",
                                                                 "100");
        Map<String, String> secondMonitorConfig = ImmutableMap.of("monitor.monitor2.factory.class",
                                                                  "org.apache.samza.monitor.DummyMonitor",
                                                                  "monitor.monitor2.scheduling.interval.ms",
                                                                  "200");
        MapConfig mapConfig = new MapConfig(ImmutableList.of(firstMonitorConfig, secondMonitorConfig));
        MonitorConfig expectedFirstConfig = new MonitorConfig(new MapConfig(firstMonitorConfig).subset("monitor.monitor1."));
        MonitorConfig expectedSecondConfig = new MonitorConfig(new MapConfig(secondMonitorConfig).subset("monitor.monitor2."));
        Map<String, MonitorConfig> expected = ImmutableMap.of("monitor1", expectedFirstConfig, "monitor2", expectedSecondConfig);
        assertEquals(expected, MonitorConfig.getMonitorConfigs(mapConfig));
    }

    @Test
    public void testMonitorExceptionIsolation() {
        // Test that an exception from a monitor doesn't bubble up out of the scheduler.
        Map<String, String> configMap =
            ImmutableMap.of(String.format("monitor.name.%s", CONFIG_MONITOR_FACTORY_CLASS),
                            ExceptionThrowingMonitorFactory.class.getCanonicalName());
        SamzaRestConfig config = new SamzaRestConfig(new MapConfig(configMap));
        SamzaMonitorService monitorService = new SamzaMonitorService(config,
                                                                     METRICS_REGISTRY,
                                                                     new InstantSchedulingProvider());

        // This will throw if the exception isn't caught within the provider.
        monitorService.start();
        monitorService.stop();
    }

    @Test
    public void testShouldNotFailWhenTheMonitorFactoryClassIsNotDefined()
        throws Exception {
        // Test that when MonitorFactoryClass is not defined in the config, monitor service
        // should not fail.
        Map<String, String> configMap = ImmutableMap.of("monitor.monitor1.config.key1", "configValue1",
                                                        "monitor.monitor1.config.key2", "configValue2",
                                                        String.format("monitor.MOCK_MONITOR.%s", CONFIG_MONITOR_FACTORY_CLASS),
                                                        MockMonitorFactory.class.getCanonicalName());

        SamzaRestConfig config = new SamzaRestConfig(new MapConfig(configMap));
        SamzaMonitorService monitorService = new SamzaMonitorService(config,
                                                                     METRICS_REGISTRY,
                                                                     new InstantSchedulingProvider());
        try {
            monitorService.start();
        } catch (Exception e) {
            fail();
        }
        Mockito.verify(MockMonitorFactory.MOCK_MONITOR, Mockito.times(1)).monitor();
    }

    @Test(expected = SamzaException.class)
    public void testShouldFailWhenTheMonitorFactoryClassIsInvalid() {
        // Test that when MonitorFactoryClass is defined in the config and is invalid,
        // monitor service should fail. Should throw back SamzaException.
        Map<String, String> configMap = ImmutableMap.of(String.format("monitor.name.%s", CONFIG_MONITOR_FACTORY_CLASS),
                                                        "RandomClassName");
        SamzaRestConfig config = new SamzaRestConfig(new MapConfig(configMap));
        SamzaMonitorService monitorService = new SamzaMonitorService(config,
                                                                     METRICS_REGISTRY,
                                                                     new InstantSchedulingProvider());
        monitorService.start();
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