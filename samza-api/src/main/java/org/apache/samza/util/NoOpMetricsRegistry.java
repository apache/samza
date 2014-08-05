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

package org.apache.samza.util;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;

/**
 * {@link org.apache.samza.metrics.MetricsRegistry} implementation for when no actual metrics need to be
 * recorded but a registry is still required.
 */
public class NoOpMetricsRegistry implements MetricsRegistry {
  @Override
  public Counter newCounter(String group, String name) {
    return new Counter(name);
  }

  @Override
  public Counter newCounter(String group, Counter counter) {
    return counter;
  }

  @Override
  public <T> Gauge<T> newGauge(String group, String name, T value) {
    return new Gauge<T>(name, value);
  }

  @Override
  public <T> Gauge<T> newGauge(String group, Gauge<T> gauge) {
    return gauge;
  }

  @Override
  public Timer newTimer(String group, String name) {
    return new Timer(name);
  }

  @Override
  public Timer newTimer(String group, Timer timer) {
    return timer;
  }
}