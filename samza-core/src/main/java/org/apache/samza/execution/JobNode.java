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

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.spec.StatefulOperatorSpec;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.table.TableConfigGenerator;
import org.apache.samza.util.MathUtil;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.table.TableSpec;
import org.apache.samza.util.StreamUtil;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;


/**
 * A JobNode is a physical execution unit. In RemoteExecutionEnvironment, it's a job that will be submitted
 * to remote cluster. In LocalExecutionEnvironment, it's a set of StreamProcessors for local execution.
 * A JobNode contains the input/output, and the configs for physical execution.
 */
public class JobNode {
  private static final Logger log = LoggerFactory.getLogger(JobNode.class);
  private static final String CONFIG_INTERNAL_EXECUTION_PLAN = "samza.internal.execution.plan";

  private final String jobName;
  private final String jobId;
  private final String id;
  private final OperatorSpecGraph specGraph;
  private final List<StreamEdge> inEdges = new ArrayList<>();
  private final List<StreamEdge> outEdges = new ArrayList<>();
  private final List<TableSpec> tables = new ArrayList<>();
  private final Config config;

  JobNode(String jobName, String jobId, OperatorSpecGraph specGraph, Config config) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.id = createId(jobName, jobId);
    this.specGraph = specGraph;
    this.config = config;
  }

  public static Config mergeJobConfig(Config fullConfig, Config generatedConfig) {
    return new JobConfig(Util.rewriteConfig(extractScopedConfig(
        fullConfig, generatedConfig, String.format(JobConfig.CONFIG_JOB_PREFIX(), new JobConfig(fullConfig).getName().get()))));
  }

  public OperatorSpecGraph getSpecGraph() {
    return this.specGraph;
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

  void addTable(TableSpec tableSpec) {
    tables.add(tableSpec);
  }

  /**
   * Generate the configs for a job
   * @param executionPlanJson JSON representation of the execution plan
   * @return config of the job
   */
  public JobConfig generateConfig(String executionPlanJson) {
    Map<String, String> configs = new HashMap<>();
    configs.put(JobConfig.JOB_NAME(), jobName);
    configs.put(JobConfig.JOB_ID(), jobId);

    final List<String> inputs = new ArrayList<>();
    final List<String> broadcasts = new ArrayList<>();
    for (StreamEdge inEdge : inEdges) {
      String formattedSystemStream = inEdge.getName();
      if (inEdge.isBroadcast()) {
        broadcasts.add(formattedSystemStream + "#0");
      } else {
        inputs.add(formattedSystemStream);
      }
    }

    if (!broadcasts.isEmpty()) {
      // TODO: remove this once we support defining broadcast input stream in high-level
      // task.broadcast.input should be generated by the planner in the future.
      final String taskBroadcasts = config.get(TaskConfigJava.BROADCAST_INPUT_STREAMS);
      if (StringUtils.isNoneEmpty(taskBroadcasts)) {
        broadcasts.add(taskBroadcasts);
      }
      configs.put(TaskConfigJava.BROADCAST_INPUT_STREAMS, Joiner.on(',').join(broadcasts));
    }

    // set triggering interval if a window or join is defined
    if (specGraph.hasWindowOrJoins()) {
      if ("-1".equals(config.get(TaskConfig.WINDOW_MS(), "-1"))) {
        long triggerInterval = computeTriggerInterval();
        log.info("Using triggering interval: {} for jobName: {}", triggerInterval, jobName);

        configs.put(TaskConfig.WINDOW_MS(), String.valueOf(triggerInterval));
      }
    }

    specGraph.getAllOperatorSpecs().forEach(opSpec -> {
        if (opSpec instanceof StatefulOperatorSpec) {
          ((StatefulOperatorSpec) opSpec).getStoreDescriptors()
              .forEach(sd -> configs.putAll(sd.getStorageConfigs()));
          // store key and message serdes are configured separately in #addSerdeConfigs
        }
      });

    configs.put(CONFIG_INTERNAL_EXECUTION_PLAN, executionPlanJson);

    // write input/output streams to configs
    inEdges.stream().filter(StreamEdge::isIntermediate).forEach(edge -> configs.putAll(edge.generateConfig()));

    // write serialized serde instances and stream serde configs to configs
    addSerdeConfigs(configs);

    configs.putAll(TableConfigGenerator.generateConfigsForTableSpecs(new MapConfig(configs), tables));

    // Add side inputs to the inputs and mark the stream as bootstrap
    tables.forEach(tableSpec -> {
        List<String> sideInputs = tableSpec.getSideInputs();
        if (sideInputs != null && !sideInputs.isEmpty()) {
          sideInputs.stream()
              .map(sideInput -> StreamUtil.getSystemStreamFromNameOrId(config, sideInput))
              .forEach(systemStream -> {
                  inputs.add(StreamUtil.getNameFromSystemStream(systemStream));
                  configs.put(String.format(StreamConfig.STREAM_PREFIX() + StreamConfig.BOOTSTRAP(),
                      systemStream.getSystem(), systemStream.getStream()), "true");
                });
        }
      });

    configs.put(TaskConfig.INPUT_STREAMS(), Joiner.on(',').join(inputs));

    log.info("Job {} has generated configs {}", jobName, configs);

    String configPrefix = String.format(JobConfig.CONFIG_JOB_PREFIX(), jobName);

    // Disallow user specified job inputs/outputs. This info comes strictly from the user application.
    Map<String, String> allowedConfigs = new HashMap<>(config);
    if (allowedConfigs.containsKey(TaskConfig.INPUT_STREAMS())) {
      log.warn("Specifying task inputs in configuration is not allowed with Fluent API. "
          + "Ignoring configured value for " + TaskConfig.INPUT_STREAMS());
      allowedConfigs.remove(TaskConfig.INPUT_STREAMS());
    }

    log.debug("Job {} has allowed configs {}", jobName, allowedConfigs);
    return new JobConfig(
        Util.rewriteConfig(
            extractScopedConfig(new MapConfig(allowedConfigs), new MapConfig(configs), configPrefix)));
  }

  /**
   * Serializes the {@link Serde} instances for operators, adds them to the provided config, and
   * sets the serde configuration for the input/output/intermediate streams appropriately.
   *
   * We try to preserve the number of Serde instances before and after serialization. However we don't
   * guarantee that references shared between these serdes instances (e.g. an Jackson ObjectMapper shared
   * between two json serdes) are shared after deserialization too.
   *
   * Ideally all the user defined objects in the application should be serialized and de-serialized in one pass
   * from the same output/input stream so that we can maintain reference sharing relationships.
   *
   * @param configs the configs to add serialized serde instances and stream serde configs to
   */
  void addSerdeConfigs(Map<String, String> configs) {
    // collect all key and msg serde instances for streams
    Map<String, Serde> streamKeySerdes = new HashMap<>();
    Map<String, Serde> streamMsgSerdes = new HashMap<>();
    Map<String, InputOperatorSpec> inputOperators = specGraph.getInputOperators();
    inEdges.forEach(edge -> {
        String streamId = edge.getStreamSpec().getId();
        InputOperatorSpec inputOperatorSpec = inputOperators.get(streamId);
        Serde keySerde = inputOperatorSpec.getKeySerde();
        if (keySerde != null) {
          streamKeySerdes.put(streamId, keySerde);
        }
        Serde valueSerde = inputOperatorSpec.getValueSerde();
        if (valueSerde != null) {
          streamMsgSerdes.put(streamId, valueSerde);
        }
      });
    Map<String, OutputStreamImpl> outputStreams = specGraph.getOutputStreams();
    outEdges.forEach(edge -> {
        String streamId = edge.getStreamSpec().getId();
        OutputStreamImpl outputStream = outputStreams.get(streamId);
        Serde keySerde = outputStream.getKeySerde();
        if (keySerde != null) {
          streamKeySerdes.put(streamId, keySerde);
        }
        Serde valueSerde = outputStream.getValueSerde();
        if (valueSerde != null) {
          streamMsgSerdes.put(streamId, valueSerde);
        }
      });

    // collect all key and msg serde instances for stores
    Map<String, Serde> storeKeySerdes = new HashMap<>();
    Map<String, Serde> storeMsgSerdes = new HashMap<>();
    specGraph.getAllOperatorSpecs().forEach(opSpec -> {
        if (opSpec instanceof StatefulOperatorSpec) {
          ((StatefulOperatorSpec) opSpec).getStoreDescriptors().forEach(storeDescriptor -> {
              storeKeySerdes.put(storeDescriptor.getStoreName(), storeDescriptor.getKeySerde());
              storeMsgSerdes.put(storeDescriptor.getStoreName(), storeDescriptor.getMsgSerde());
            });
        }
      });

    // for each unique stream or store serde instance, generate a unique name and serialize to config
    HashSet<Serde> serdes = new HashSet<>(streamKeySerdes.values());
    serdes.addAll(streamMsgSerdes.values());
    serdes.addAll(storeKeySerdes.values());
    serdes.addAll(storeMsgSerdes.values());
    SerializableSerde<Serde> serializableSerde = new SerializableSerde<>();
    Base64.Encoder base64Encoder = Base64.getEncoder();
    Map<Serde, String> serdeUUIDs = new HashMap<>();
    serdes.forEach(serde -> {
        String serdeName = serdeUUIDs.computeIfAbsent(serde,
            s -> serde.getClass().getSimpleName() + "-" + UUID.randomUUID().toString());
        configs.putIfAbsent(String.format(SerializerConfig.SERDE_SERIALIZED_INSTANCE(), serdeName),
            base64Encoder.encodeToString(serializableSerde.toBytes(serde)));
      });

    // set key and msg serdes for streams to the serde names generated above
    streamKeySerdes.forEach((streamId, serde) -> {
        String streamIdPrefix = String.format(StreamConfig.STREAM_ID_PREFIX(), streamId);
        String keySerdeConfigKey = streamIdPrefix + StreamConfig.KEY_SERDE();
        configs.put(keySerdeConfigKey, serdeUUIDs.get(serde));
      });

    streamMsgSerdes.forEach((streamId, serde) -> {
        String streamIdPrefix = String.format(StreamConfig.STREAM_ID_PREFIX(), streamId);
        String valueSerdeConfigKey = streamIdPrefix + StreamConfig.MSG_SERDE();
        configs.put(valueSerdeConfigKey, serdeUUIDs.get(serde));
      });

    // set key and msg serdes for stores to the serde names generated above
    storeKeySerdes.forEach((storeName, serde) -> {
        String keySerdeConfigKey = String.format(StorageConfig.KEY_SERDE(), storeName);
        configs.put(keySerdeConfigKey, serdeUUIDs.get(serde));
      });

    storeMsgSerdes.forEach((storeName, serde) -> {
        String msgSerdeConfigKey = String.format(StorageConfig.MSG_SERDE(), storeName);
        configs.put(msgSerdeConfigKey, serdeUUIDs.get(serde));
      });
  }

  /**
   * Computes the triggering interval to use during the execution of this {@link JobNode}
   */
  private long computeTriggerInterval() {
    // Obtain the operator specs from the specGraph
    Collection<OperatorSpec> operatorSpecs = specGraph.getAllOperatorSpecs();

    // Filter out window operators, and obtain a list of their triggering interval values
    List<Long> windowTimerIntervals = operatorSpecs.stream()
        .filter(spec -> spec.getOpCode() == OperatorSpec.OpCode.WINDOW)
        .map(spec -> ((WindowOperatorSpec) spec).getDefaultTriggerMs())
        .collect(Collectors.toList());

    // Filter out the join operators, and obtain a list of their ttl values
    List<Long> joinTtlIntervals = operatorSpecs.stream()
        .filter(spec -> spec instanceof JoinOperatorSpec)
        .map(spec -> ((JoinOperatorSpec) spec).getTtlMs())
        .collect(Collectors.toList());

    // Combine both the above lists
    List<Long> candidateTimerIntervals = new ArrayList<>(joinTtlIntervals);
    candidateTimerIntervals.addAll(windowTimerIntervals);

    if (candidateTimerIntervals.isEmpty()) {
      return -1;
    }

    // Compute the gcd of the resultant list
    return MathUtil.gcd(candidateTimerIntervals);
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

  static String createId(String jobName, String jobId) {
    return String.format("%s-%s", jobName, jobId);
  }
}
