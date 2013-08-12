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

package org.apache.samza.metrics;

import java.util.concurrent.atomic.AtomicReference;

public class Gauge<T> implements Metric {
  private final String name;
  private AtomicReference<T> ref;

  public Gauge(String name, T value) {
    this.name = name;
    this.ref = new AtomicReference<T>(value);
  }

  public T set(T n) {
    return ref.getAndSet(n);
  }

  public T getValue() {
    return ref.get();
  }

  public String getName() {
    return name;
  }

  public void visit(MetricsVisitor visitor) {
    visitor.gauge(this);
  }

  public String toString() {
    T value = ref.get();
    return (value == null) ? null : value.toString();
  }
}
