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
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.task.StreamTaskFactory;

import java.util.List;

public class StreamTaskApplication extends ApplicationBase {

  final StreamTaskFactory taskFactory;
  final Config config;
  private final List<StreamDescriptor.Input> taskInputs = new ArrayList<>();
  private final List<StreamDescriptor.Output> taskOutputs = new ArrayList<>();

  private StreamTaskApplication(StreamTaskFactory taskFactory, ApplicationRunner runner, Config config) {
    super(runner);
    this.taskFactory = taskFactory;
    this.config = config;
  }

  public static StreamTaskApplication create(Config config, StreamTaskFactory taskFactory) {
    ApplicationRunner runner = ApplicationRunner.fromConfig(config);
    return new StreamTaskApplication(taskFactory, runner, config);
  }

  public StreamTaskApplication addInputs(List<StreamDescriptor.Input> inputs) {
    this.taskInputs.addAll(inputs);
    return this;
  }

  public StreamTaskApplication addOutputs(List<StreamDescriptor.Output> outputs) {
    this.taskOutputs.addAll(outputs);
    return this;
  }

  public StreamTaskApplication withMetricsReporters(Map<String, MetricsReporter> metrics) {
    this.withMetricsReports(metrics);
    return this;
  }

}
