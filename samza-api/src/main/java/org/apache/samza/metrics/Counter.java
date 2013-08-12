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

import java.util.concurrent.atomic.AtomicLong;

/**
 * A counter is a metric that represents a cumulative value.
 */
public class Counter implements Metric {
  private final String name;
  private final AtomicLong count;

  public Counter(String name) {
    this.name = name;
    this.count = new AtomicLong(0);
  }

  public long inc() {
    return inc(1);
  }

  public long inc(long n) {
    return count.addAndGet(n);
  }

  public long dec() {
    return dec(1);
  }

  public long dec(long n) {
    return count.addAndGet(0 - n);
  }
  
  public void set(long n) {
    count.set(n);
  }

  public void clear() {
    count.set(0);
  }

  public long getCount() {
    return count.get();
  }

  public String getName() {
    return name;
  }

  public void visit(MetricsVisitor visitor) {
    visitor.counter(this);
  }

  public String toString() {
    return count.toString();
  }
}
