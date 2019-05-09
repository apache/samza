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

import java.io.File;
import java.util.Optional;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.runtime.LocalContainerRunner;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;


public class DiagnosticsUtil {
  private static final Logger log = LoggerFactory.getLogger(DiagnosticsUtil.class);

  public static void writeMetadataFile(String jobName, String jobId, String containerId,
      Optional<String> execEnvContainerId, Config config) throws Exception {

    Option<File> metadataFile = JobConfig.getMetadataFile(Option.apply(execEnvContainerId.orElse(null)));

    if (metadataFile.isDefined()) {

      StringBuilder metadata = new StringBuilder("Version: 1");
      metadata.append(System.lineSeparator());
      MetricsHeader metricsHeader =
          new MetricsHeader(jobName, jobId, "samza-container-" + containerId, execEnvContainerId.orElse(""), LocalContainerRunner.class.getName(),
              Util.getTaskClassVersion(config), Util.getSamzaVersion(), Util.getLocalHost().getHostName(),
              System.currentTimeMillis(), System.currentTimeMillis());

      MetricsSnapshot metricsSnapshot = new MetricsSnapshot(metricsHeader, new Metrics());
      metadata.append("MetricsSnapshot: ");
      metadata.append(SamzaObjectMapper.getObjectMapper().writeValueAsString(metricsSnapshot));
      FileUtil.writeToTextFile(metadataFile.get(), metadata.toString(), false);
    } else {
      log.info("Skipping writing metadata file.");
    }
  }
}
