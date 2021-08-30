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

import java.util.Set;
import com.google.common.base.Preconditions;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;


/**
 * Common place for reading configs and wiring {@link StreamPartitionCountMonitor}.
 */
public class StreamPartitionCountMonitorFactory {
  private final StreamMetadataCache streamMetadataCache;
  private final MetricsRegistry metrics;

  public StreamPartitionCountMonitorFactory(StreamMetadataCache streamMetadataCache, MetricsRegistry metrics) {
    this.streamMetadataCache = Preconditions.checkNotNull(streamMetadataCache);
    this.metrics = Preconditions.checkNotNull(metrics);
  }

  /**
   * Build {@link StreamPartitionCountMonitor} for input streams.
   */
  public StreamPartitionCountMonitor build(Config config, StreamPartitionCountMonitor.Callback callback) {
    Set<SystemStream> inputStreamsToMonitor = new TaskConfig(config).getAllInputStreams();
    if (inputStreamsToMonitor.isEmpty()) {
      throw new SamzaException("Input streams to a job can not be empty.");
    }
    return new StreamPartitionCountMonitor(inputStreamsToMonitor, this.streamMetadataCache, this.metrics,
        new JobConfig(config).getMonitorPartitionChangeFrequency(), callback);
  }
}
