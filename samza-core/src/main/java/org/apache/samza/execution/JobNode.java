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

package org.apache.samza.execution;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A ProcessorNode is a physical execution unit. In RemoteExecutionEnvironment, it's a job that will be submitted
 * to remote cluster. In LocalExecutionEnvironment, it's a set of StreamProcessors for local execution.
 * A ProcessorNode contains the input/output, and the configs for physical execution.
 */
public class JobNode {
  private static final Logger log = LoggerFactory.getLogger(JobNode.class);
  private static final String CONFIG_PROCESSOR_PREFIX = "processors.%s.";

  private final String jobName;
  private final String jobId;
  private final String id;
  private final List<StreamEdge> inEdges = new ArrayList<>();
  private final List<StreamEdge> outEdges = new ArrayList<>();
  private final Config config;

  JobNode(String jobName, String jobId, Config config) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.id = createId(jobName, jobId);
    this.config = config;
  }

  public  String getId() {
    return id;
  }

  public String getJobName() {
    return jobName;
  }

  public String getJobId() {
    return jobId;
  }

  void addInEdge(StreamEdge in) {
    inEdges.add(in);
  }

  void addOutEdge(StreamEdge out) {
    outEdges.add(out);
  }

  List<StreamEdge> getInEdges() {
    return inEdges;
  }

  List<StreamEdge> getOutEdges() {
    return outEdges;
  }

  public Config generateConfig() {
    Map<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), id);

    List<String> inputs = inEdges.stream().map(edge -> edge.getFormattedSystemStream()).collect(Collectors.toList());
    configs.put(TaskConfig.INPUT_STREAMS(), Joiner.on(',').join(inputs));
    log.info("Processor {} has generated configs {}", id, configs);

    String configPrefix = String.format(CONFIG_PROCESSOR_PREFIX, id);
    // TODO: Disallow user specifying processor inputs/outputs. This info comes strictly from the pipeline.
    return Util.rewriteConfig(extractScopedConfig(config, new MapConfig(configs), configPrefix));
  }

  /**
   * This function extract the subset of configs from the full config, and use it to override the generated configs
   * from the processor.
   * @param fullConfig full config
   * @param generatedConfig config generated from the processor
   * @param configPrefix prefix to extract the subset of the config overrides
   * @return config that merges the generated configs and overrides
   */
  private static Config extractScopedConfig(Config fullConfig, Config generatedConfig, String configPrefix) {
    Config scopedConfig = fullConfig.subset(configPrefix);

    Config[] configPrecedence = new Config[] {fullConfig, generatedConfig, scopedConfig};
    // Strip empty configs so they don't override the configs before them.
    Map<String, String> mergedConfig = new HashMap<>();
    for (Map<String, String> config : configPrecedence) {
      for (Map.Entry<String, String> property : config.entrySet()) {
        String value = property.getValue();
        if (!(value == null || value.isEmpty())) {
          mergedConfig.put(property.getKey(), property.getValue());
        }
      }
    }
    scopedConfig = new MapConfig(mergedConfig);
    log.debug("Prefix '{}' has merged config {}", configPrefix, scopedConfig);

    return scopedConfig;
  }

  static String createId(String jobName, String jobId) {
    return String.format("%s-%s", jobName, jobId);
  }
}
