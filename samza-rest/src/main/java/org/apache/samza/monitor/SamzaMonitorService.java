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

import com.google.common.base.Strings;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.rest.SamzaRestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.samza.monitor.MonitorConfig.getMonitorConfigs;
import static org.apache.samza.monitor.MonitorLoader.instantiateMonitor;


/**
 * The class responsible for handling long-running/scheduled monitors in Samza REST.
 * Takes a SamzaRestConfig object in the constructor and handles instantiation of
 * monitors and scheduling them to run based on the properties in the config.
 */
public class SamzaMonitorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SamzaMonitorService.class);

    private final SchedulingProvider scheduler;
    private final SamzaRestConfig config;
    private final MetricsRegistry metricsRegistry;

    public SamzaMonitorService(SamzaRestConfig config,
                               MetricsRegistry metricsRegistry,
                               SchedulingProvider schedulingProvider) {
        this.config = config;
        this.metricsRegistry = metricsRegistry;
        this.scheduler = schedulingProvider;
    }

    public void start() {
        try {
            Map<String, MonitorConfig> monitorConfigs = getMonitorConfigs(config);
            for (Map.Entry<String, MonitorConfig> entry : monitorConfigs.entrySet()) {
                String monitorName = entry.getKey();
                MonitorConfig monitorConfig = entry.getValue();

                if (!Strings.isNullOrEmpty(monitorConfig.getMonitorFactoryClass())) {
                    int schedulingIntervalInMs = monitorConfig.getSchedulingIntervalInMs();
                    LOGGER.info("Scheduling monitor {} to run every {} ms", monitorName, schedulingIntervalInMs);
                    // MetricsRegistry has been added in the Monitor interface, since it's required in the eventual future to record metrics.
                    // We have plans to record metrics, hence adding this as a placeholder. We just aren't doing it yet.
                    scheduler.schedule(getRunnable(instantiateMonitor(monitorConfig, metricsRegistry)), schedulingIntervalInMs);
                } else {
                  // When MonitorFactoryClass is not defined in the config, ignore the monitor config
                  LOGGER.warn("Not scheduling the monitor: {} to run, since monitor factory class is not set in config.", monitorName);
                }
            }
        } catch (InstantiationException e) {
            LOGGER.error("Exception when instantiating the monitor : ", e);
            throw new SamzaException(e);
        }
    }

    public void stop() {
        this.scheduler.stop();
    }

    private Runnable getRunnable(final Monitor monitor) {
        return new Runnable() {
            public void run() {
                try {
                    monitor.monitor();
                } catch (IOException e) {
                    LOGGER.error("Caught IOException during " + monitor.toString() + ".monitor()", e);
                } catch (InterruptedException e) {
                    LOGGER.error("Caught InterruptedException during " + monitor.toString() + ".monitor()", e);
                } catch (Exception e) {
                    LOGGER.error("Unexpected exception during {}.monitor()", monitor, e);
                }
            }
        };
    }
}
