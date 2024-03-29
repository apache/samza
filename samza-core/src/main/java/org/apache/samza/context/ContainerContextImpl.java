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
package org.apache.samza.context;

import java.util.concurrent.ExecutorService;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.metrics.MetricsRegistry;


public class ContainerContextImpl implements ContainerContext {
  private final ContainerModel containerModel;
  private final MetricsRegistry containerMetricsRegistry;

  private final ExecutorService containerThreadPool;

  public ContainerContextImpl(ContainerModel containerModel, MetricsRegistry containerMetricsRegistry,
      ExecutorService containerThreadPool) {
    this.containerModel = containerModel;
    this.containerMetricsRegistry = containerMetricsRegistry;
    this.containerThreadPool = containerThreadPool;
  }

  @Override
  public ContainerModel getContainerModel() {
    return this.containerModel;
  }

  @Override
  public MetricsRegistry getContainerMetricsRegistry() {
    return this.containerMetricsRegistry;
  }

  @Override
  public ExecutorService getContainerThreadPool() {
    return this.containerThreadPool;
  }
}
