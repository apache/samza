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
package org.apache.samza.application;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.application.internal.ApplicationSpec;
import org.apache.samza.application.internal.StreamApplicationSpec;
import org.apache.samza.application.internal.TaskApplicationSpec;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.runtime.internal.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.task.TaskFactory;


/**
 * This class defines the methods to create different types of Samza application instances: 1) high-level end-to-end
 * {@link StreamApplication}; 2) task-level StreamTaskApplication; 3) task-level AsyncStreamTaskApplication
 */
public final class StreamApplications {
  // The static map of all created application instances from the user program
  private static final Map<String, ApplicationRunnable> USER_APPS = new HashMap<>();

  private StreamApplications() {

  }

  public static final ApplicationRunnable createRunnable(StreamApplication userApp, Config config) {
    StreamGraph graph = StreamGraph.createInstance();
    userApp.init(graph, config);
    ApplicationRuntimeInstance appRuntime = new ApplicationRuntimeInstance(graph, config);
    USER_APPS.put(appRuntime.getGlobalAppId(), appRuntime);
    return appRuntime;
  }

  public static final ApplicationRunnable createRunnable(TaskFactory taskFactory, Config config) {
    ApplicationRuntimeInstance appRuntime = new ApplicationRuntimeInstance(taskFactory, config);
    USER_APPS.put(appRuntime.getGlobalAppId(), appRuntime);
    return appRuntime;
  }

  static final class ApplicationRuntimeInstance implements ApplicationRunnable {

    /*package private*/
    final ApplicationRunner runner;
    final ApplicationSpec appSpec;

    private ApplicationRuntimeInstance(ApplicationSpec appSpec, Config config) {
      this.appSpec = appSpec;
      this.runner = ApplicationRunners.fromConfig(config);
    }

    ApplicationRuntimeInstance(StreamGraph graph, Config config) {
      this(new StreamApplicationSpec(graph, config), config);
    }

    ApplicationRuntimeInstance(TaskFactory taskFactory, Config config) {
      this(new TaskApplicationSpec(taskFactory, config), config);
    }

    @Override
    public final void run() {
      this.runner.run(this.appSpec);
    }

    @Override
    public final void kill() {
      this.runner.kill(this.appSpec);
    }

    @Override
    public final ApplicationStatus status() {
      return this.runner.status(this.appSpec);
    }

    @Override
    public final void waitForFinish() {
      this.runner.waitForFinish(this.appSpec);
    }

    @Override
    public final boolean waitForFinish(Duration timeout) {
      return this.runner.waitForFinish(this.appSpec, timeout);
    }

    /**
     * Set {@link MetricsReporter}s for this {@link ApplicationRuntimeInstance}
     *
     * @param metricsReporters the map of {@link MetricsReporter}s to be added
     * @return this {@link ApplicationRuntimeInstance} instance
     */
    @Override
    public final ApplicationRuntimeInstance withMetricsReporters(Map<String, MetricsReporter> metricsReporters) {
      this.runner.addMetricsReporters(metricsReporters);
      return this;
    }

    String getGlobalAppId() {
      return this.appSpec.getGlobalAppId();
    }
  }
}
