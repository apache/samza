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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.util.MathUtils;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A JobNode is a physical execution unit. In RemoteExecutionEnvironment, it's a job that will be submitted
 * to remote cluster. In LocalExecutionEnvironment, it's a set of StreamProcessors for local execution.
 * A JobNode contains the input/output, and the configs for physical execution.
 */
public class JobNode {
  private static final Logger log = LoggerFactory.getLogger(JobNode.class);
  private static final String CONFIG_JOB_PREFIX = "jobs.%s.";

  private final String jobName;
  private final String jobId;
  private final String id;
  private final StreamGraphImpl streamGraph;
  private final List<StreamEdge> inEdges = new ArrayList<>();
  private final List<StreamEdge> outEdges = new ArrayList<>();
  private final Config config;

  JobNode(String jobName, String jobId, StreamGraphImpl streamGraph, Config config) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.id = createId(jobName, jobId);
    this.streamGraph = streamGraph;
    this.config = config;
  }

  public StreamGraphImpl getStreamGraph() {
    return streamGraph;
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

  /**
   * Generate the configs for a job
   * @param otherConfigs other configs that apply to the job
   * @return config of the job
   */
  public JobConfig generateConfig(Map<String, String> otherConfigs) {
    Map<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), jobName);

    List<String> inputs = inEdges.stream().map(edge -> edge.getFormattedSystemStream()).collect(Collectors.toList());
    configs.put(TaskConfig.INPUT_STREAMS(), Joiner.on(',').join(inputs));

    // set triggering interval if a window or join is defined
    if (streamGraph.hasWindowOrJoins()) {
      if ("-1".equals(config.get(TaskConfig.WINDOW_MS(), "-1"))) {
        long triggerInterval = computeTriggerInterval();
        log.info("Using triggering interval: {} for jobName: {}", triggerInterval, jobName);

        configs.put(TaskConfig.WINDOW_MS(), String.valueOf(triggerInterval));
      }
    }

    configs.putAll(otherConfigs);

    // write input/output streams to configs
    inEdges.stream().filter(StreamEdge::isIntermediate).forEach(edge -> addStreamConfig(edge, configs));

    log.info("Job {} has generated configs {}", jobName, configs);

    String configPrefix = String.format(CONFIG_JOB_PREFIX, jobName);
    // TODO: Disallow user specifying job inputs/outputs. This info comes strictly from the pipeline.
    return new JobConfig(Util.rewriteConfig(extractScopedConfig(config, new MapConfig(configs), configPrefix)));
  }

  /**
   * Computes the triggering interval to use during the execution of this {@link JobNode}
   */
  private long computeTriggerInterval() {
    // Obtain the operator specs from the streamGraph
    Collection<OperatorSpec> operatorSpecs = streamGraph.getAllOperatorSpecs();

    // Filter out window operators, and obtain a list of their triggering interval values
    List<Long> windowTimerIntervals = operatorSpecs.stream()
        .filter(spec -> spec.getOpCode() == OperatorSpec.OpCode.WINDOW)
        .map(spec -> ((WindowOperatorSpec) spec).getDefaultTriggerMs())
        .collect(Collectors.toList());

    // Filter out the join operators, and obtain a list of their ttl values
    List<Long> joinTtlIntervals = operatorSpecs.stream()
        .filter(spec -> spec.getOpCode() == OperatorSpec.OpCode.JOIN)
        .map(spec -> ((JoinOperatorSpec) spec).getTtlMs())
        .collect(Collectors.toList());

    // Combine both the above lists
    List<Long> candidateTimerIntervals = new ArrayList<>(joinTtlIntervals);
    candidateTimerIntervals.addAll(windowTimerIntervals);

    // Compute the gcd of the resultant list
    long timerInterval = MathUtils.gcd(candidateTimerIntervals);
    return timerInterval;
  }

  /**
   * This function extract the subset of configs from the full config, and use it to override the generated configs
   * from the job.
   * @param fullConfig full config
   * @param generatedConfig config generated for the job
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

  private static void addStreamConfig(StreamEdge edge, Map<String, String> config) {
    StreamSpec spec = edge.getStreamSpec();
    config.put(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID(), spec.getId()), spec.getSystemName());
    config.put(String.format(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID(), spec.getId()), spec.getPhysicalName());
    if (edge.isIntermediate()) {
      config.put(String.format(StreamConfig.IS_INTERMEDIATE_FOR_STREAM_ID(), spec.getId()), "true");
    }
    spec.getConfig().forEach((property, value) -> {
        config.put(String.format(StreamConfig.STREAM_ID_PREFIX(), spec.getId()) + property, value);
      });
  }

  static String createId(String jobName, String jobId) {
    return String.format("%s-%s", jobName, jobId);
  }
}
