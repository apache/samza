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

import org.apache.samza.rest.SamzaRestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * The class responsible for handling long-running/scheduled monitors in Samza REST.
 * Takes a SamzaRestConfig object in the constructor and handles instantiation of
 * monitors and scheduling them to run based on the properties in the config.
 */
public class SamzaMonitorService {

    private static final Logger log = LoggerFactory.getLogger(SamzaMonitorService.class);

    private final SchedulingProvider scheduler;
    private final SamzaRestConfig config;

    public SamzaMonitorService(SamzaRestConfig config, SchedulingProvider schedulingProvider) {
        this.scheduler = schedulingProvider;
        this.config = config;
    }

    public void start() {
        List<Monitor> monitors = getMonitorsFromConfig(config);
        int monitorRunInterval = config.getConfigMonitorIntervalMs();
        for (Monitor monitor : monitors) {
            log.debug("Scheduling monitor {} to run every {}ms", monitor, monitorRunInterval);
            this.scheduler.schedule(getRunnable(monitor), monitorRunInterval);
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
                    log.warn("Caught IOException during " + monitor.toString() + ".monitor()", e);
                } catch (InterruptedException e) {
                    log.warn("Caught InterruptedException during " + monitor.toString() + ".monitor()", e);
                } catch (Exception e) {
                    log.warn("Unexpected exception during {}.monitor()", monitor, e);
                }
            }
        };
    }

    /**
     * Get all the registered monitors for the service.
     * @return a list of Monitor objects ready to be scheduled.
     */
    private static List<Monitor> getMonitorsFromConfig(SamzaRestConfig config) {
        List<String> classNames = config.getConfigMonitorClassList();
        List<Monitor> monitors = new ArrayList<>();

        for (String name: classNames) {
            try {
                Monitor monitor = MonitorLoader.fromClassName(name);
                monitors.add(monitor);
            } catch (InstantiationException e) {
                log.warn("Unable to instantiate monitor " + name, e);
            }
        }
        return monitors;
    }

}
