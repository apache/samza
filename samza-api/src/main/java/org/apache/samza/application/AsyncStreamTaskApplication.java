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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.task.AsyncStreamTaskFactory;


public class AsyncStreamTaskApplication extends ApplicationBase {
  final AsyncStreamTaskFactory taskFactory;
  private final List<StreamDescriptor.Input> taskInputs = new ArrayList<>();
  private final List<StreamDescriptor.Output> taskOutputs = new ArrayList<>();

  private AsyncStreamTaskApplication(AsyncStreamTaskFactory taskFactory, ApplicationRunner runner) {
    super(runner);
    this.taskFactory = taskFactory;
  }

  public static AsyncStreamTaskApplication create(Config config, AsyncStreamTaskFactory taskFactory) {
    ApplicationRunner runner = ApplicationRunner.fromConfig(config);
    return new AsyncStreamTaskApplication(taskFactory, runner);
  }

  public AsyncStreamTaskApplication addInputs(List<StreamDescriptor.Input> inputs) {
    this.taskInputs.addAll(inputs);
    return this;
  }

  public AsyncStreamTaskApplication addOutputs(List<StreamDescriptor.Output> outputs) {
    this.taskOutputs.addAll(outputs);
    return this;
  }

  public AsyncStreamTaskApplication withMetricsReporters(Map<String, MetricsReporter> metrics) {
    this.withMetricsReports(metrics);
    return this;
  }

}
