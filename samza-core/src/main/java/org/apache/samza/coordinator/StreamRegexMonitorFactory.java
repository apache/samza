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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.samza.SamzaException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.JobModelUtil;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamRegexMonitorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(StreamRegexMonitorFactory.class);

  private final StreamMetadataCache streamMetadataCache;
  private final MetricsRegistry metrics;

  public StreamRegexMonitorFactory(StreamMetadataCache streamMetadataCache, MetricsRegistry metrics) {
    this.streamMetadataCache = streamMetadataCache;
    this.metrics = metrics;
  }

  public Optional<StreamRegexMonitor> build(JobModel jobModel, StreamRegexMonitor.Callback callback) {
    JobConfig jobConfig = new JobConfig(jobModel.getConfig());

    // if input regex monitor is not enabled return empty
    if (jobConfig.getMonitorRegexDisabled()) {
      LOG.info("StreamRegexMonitor is disabled.");
      return Optional.empty();
    }

    Set<SystemStream> inputStreamsToMonitor = JobModelUtil.getSystemStreams(jobModel);
    if (inputStreamsToMonitor.isEmpty()) {
      throw new SamzaException("Input streams to a job can not be empty.");
    }

    // First list all rewriters
    Optional<String> rewritersList = jobConfig.getConfigRewriters();

    // if no rewriter is defined, there is nothing to monitor
    if (!rewritersList.isPresent()) {
      LOG.warn("No config rewriters are defined. No StreamRegexMonitor created.");
      return Optional.empty();
    }

    // Compile a map of each input-system to its corresponding input-monitor-regex patterns
    Map<String, Pattern> inputRegexesToMonitor = jobConfig.getMonitorRegexPatternMap(rewritersList.get());

    // if there are no regexes to monitor
    if (inputRegexesToMonitor.isEmpty()) {
      LOG.info("No input regexes are defined. No StreamRegexMonitor created.");
      return Optional.empty();
    }

    return Optional.of(
        new StreamRegexMonitor(inputStreamsToMonitor, inputRegexesToMonitor, this.streamMetadataCache, this.metrics,
            jobConfig.getMonitorRegexFrequency(), callback));
  }
}
