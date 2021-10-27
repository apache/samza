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
package org.apache.samza.coordinator;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for managing multiple monitors.
 */
public class JobModelMonitors {
  private static final Logger LOG = LoggerFactory.getLogger(JobModelMonitors.class);

  private final Optional<StreamPartitionCountMonitor> streamPartitionCountMonitor;
  private final Optional<StreamRegexMonitor> streamRegexMonitor;

  private final AtomicBoolean started = new AtomicBoolean(false);

  public JobModelMonitors(StreamPartitionCountMonitor streamPartitionCountMonitor,
      StreamRegexMonitor streamRegexMonitor) {
    this.streamPartitionCountMonitor = Optional.ofNullable(streamPartitionCountMonitor);
    this.streamRegexMonitor = Optional.ofNullable(streamRegexMonitor);
  }

  public void start() {
    if (this.started.compareAndSet(false, true)) {
      this.streamPartitionCountMonitor.ifPresent(StreamPartitionCountMonitor::start);
      this.streamRegexMonitor.ifPresent(StreamRegexMonitor::start);
    } else {
      LOG.warn("Monitors already started");
    }
  }

  public void stop() {
    if (this.started.compareAndSet(true, false)) {
      this.streamPartitionCountMonitor.ifPresent(StreamPartitionCountMonitor::stop);
      this.streamRegexMonitor.ifPresent(StreamRegexMonitor::stop);
    } else {
      LOG.warn("Monitors already stopped");
    }
  }
}
