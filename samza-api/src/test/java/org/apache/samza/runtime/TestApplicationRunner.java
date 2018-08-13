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
import org.apache.samza.application.internal.AppDescriptorImpl;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;


/**
 * Test class for {@link org.apache.samza.runtime.ApplicationRunners} unit test
 */
public class TestApplicationRunner implements ApplicationRunner {

  public TestApplicationRunner(AppDescriptorImpl appDesc) {

  }

  @Override
  public void run() {

  }

  @Override
  public void kill() {

  }

  @Override
  public ApplicationStatus status() {
    return null;
  }

  @Override
  public void waitForFinish() {

  }

  @Override
  public boolean waitForFinish(Duration timeout) {
    return false;
  }

  @Override
  public void addMetricsReporters(Map<String, MetricsReporter> metricsReporters) {

  }
}
