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
package org.apache.samza.runtime.internal;

import java.time.Duration;
import java.util.Map;
import org.apache.samza.application.ApplicationSpec;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;


/**
 * Test class for {@link ApplicationRunners} unit test
 */
public class TestApplicationRunner implements ApplicationRunner {

  public TestApplicationRunner(Config config) {

  }

  @Override
  public void run(ApplicationSpec appSpec) {

  }

  @Override
  public void kill(ApplicationSpec appSpec) {

  }

  @Override
  public ApplicationStatus status(ApplicationSpec appSpec) {
    return null;
  }

  @Override
  public void waitForFinish(ApplicationSpec appSpec) {

  }

  @Override
  public boolean waitForFinish(ApplicationSpec appSpec, Duration timeout) {
    return false;
  }

  @Override
  public void addMetricsReporters(Map<String, MetricsReporter> metricsReporters) {

  }
}
