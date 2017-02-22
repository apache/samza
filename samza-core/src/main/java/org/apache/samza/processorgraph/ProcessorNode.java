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

package org.apache.samza.processorgraph;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.util.ConfigInheritence;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ProcessorNode represents a Samza processor.
 * It contains the input/output, and the config to run the processor.
 */
public class ProcessorNode {
  private static final Logger log = LoggerFactory.getLogger(ProcessorNode.class);
  private static final String CONFIG_PROCESSOR_PREFIX = "processors.%s.";

  private final String id;
  private final List<StreamEdge> inEdges = new ArrayList<>();
  private final List<StreamEdge> outEdges = new ArrayList<>();
  private final Config config;

  ProcessorNode(String id, Config config) {
    this.id = id;
    this.config = config;
  }

  public  String getId() {
    return id;
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
    String configPrefix = String.format(CONFIG_PROCESSOR_PREFIX, id);
    // TODO: Disallow user specifying processor inputs/outputs. This info comes strictly from the pipeline.
    return Util.rewriteConfig(ConfigInheritence.extractScopedConfig(config, generateProcessorConfig(), configPrefix));
  }

  private Config generateProcessorConfig() {
    Map<String, String> configs = new HashMap<>();
    List<String> inputs = inEdges.stream().map(edge -> edge.getFormattedSystemStream()).collect(Collectors.toList());

    // TODO temp logs for debugging
    log.info("Processor {} has formatted inputs {}", id, inputs);

    // TODO hack alert: hard coded string literals!
    configs.put("task.inputs", Joiner.on(',').join(inputs));

    // TODO: DISCUSS how does the processor know it's output names?
    outEdges.forEach(edge -> {
        if (!edge.getName().isEmpty()) {
          configs.put(String.format("task.outputs.%s.stream", edge.getName()), edge.getFormattedSystemStream());
        }
      });

    configs.put(JobConfig.JOB_NAME(), id);

    log.info("Processor {} has generated configs {}", id, configs);
    return new MapConfig(configs);
  }
}