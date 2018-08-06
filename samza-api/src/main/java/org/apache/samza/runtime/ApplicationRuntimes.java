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
package org.apache.samza.runtime;

import java.time.Duration;
import java.util.Map;
import org.apache.samza.application.ApplicationSpec;
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.internal.StreamAppSpecImpl;
import org.apache.samza.application.internal.TaskAppSpecImpl;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.runtime.internal.ApplicationRunner;
import org.apache.samza.runtime.internal.ApplicationRunners;


/**
 * Creates {@link ApplicationRuntime} instances based on configuration and user-implemented {@link ApplicationBase}
 */
public class ApplicationRuntimes {

  private ApplicationRuntimes() {

  }

  public static final ApplicationRuntime getApplicationRuntime(ApplicationBase userApp, Config config) {
    if (userApp instanceof StreamApplication) {
      return new AppRuntimeImpl(new StreamAppSpecImpl((StreamApplication) userApp, config));
    }
    if (userApp instanceof TaskApplication) {
      return new AppRuntimeImpl(new TaskAppSpecImpl((TaskApplication) userApp, config));
    }
    throw new IllegalArgumentException(String.format("User application instance has to be either StreamApplicationFactory or TaskApplicationFactory. "
        + "Invalid userApp class %s.", userApp.getClass().getName()));
  }

  private static class AppRuntimeImpl implements ApplicationRuntime {
    private final ApplicationSpec appSpec;
    private final ApplicationRunner runner;

    AppRuntimeImpl(ApplicationSpec appSpec) {
      this.appSpec = appSpec;
      this.runner = ApplicationRunners.fromConfig(appSpec.getConfig());
    }

    @Override
    public void run() {
      this.runner.run(appSpec);
    }

    @Override
    public void kill() {
      this.runner.kill(appSpec);
    }

    @Override
    public ApplicationStatus status() {
      return this.runner.status(appSpec);
    }

    @Override
    public void waitForFinish() {
      this.runner.waitForFinish(appSpec, Duration.ofSeconds(0));
    }

    @Override
    public boolean waitForFinish(Duration timeout) {
      return this.runner.waitForFinish(appSpec, timeout);
    }

    @Override
    public void addMetricsReporters(Map<String, MetricsReporter> metricsReporters) {
      this.runner.addMetricsReporters(metricsReporters);
    }
  }
}
