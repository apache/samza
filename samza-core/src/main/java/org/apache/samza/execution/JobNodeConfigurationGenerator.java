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
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.StatefulOperatorSpec;
import org.apache.samza.operators.spec.StoreDescriptor;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.table.TableConfigGenerator;
import org.apache.samza.table.descriptors.LocalTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.util.MathUtil;
import org.apache.samza.util.StreamUtil;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides methods to generate configuration for a {@link JobNode}
 */
/* package private */ class JobNodeConfigurationGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(JobNodeConfigurationGenerator.class);

  static final String CONFIG_INTERNAL_EXECUTION_PLAN = "samza.internal.execution.plan";

  static Config mergeConfig(Map<String, String> originalConfig, Map<String, String> generatedConfig) {
    validateJobConfigs(originalConfig, generatedConfig);
    Map<String, String> mergedConfig = new HashMap<>(generatedConfig);

    originalConfig.forEach((k, v) -> {
        if (generatedConfig.containsKey(k) && !Objects.equals(generatedConfig.get(k), v)) {
          LOG.info("Replacing generated config for key: {} value: {} with original config value: {}", k, generatedConfig.get(k), v);
        }
        mergedConfig.put(k, v);
      });

    return Util.rewriteConfig(new MapConfig(mergedConfig));
  }

  static void validateJobConfigs(Map<String, String> originalConfig, Map<String, String> generatedConfig) {
    String userConfiguredJobId = originalConfig.get(JobConfig.JOB_ID());
    String userConfiguredJobName = originalConfig.get(JobConfig.JOB_NAME());
    String generatedJobId = generatedConfig.get(JobConfig.JOB_ID());
    String generatedJobName = generatedConfig.get(JobConfig.JOB_NAME());

    if (generatedJobName != null && userConfiguredJobName != null && !StringUtils.equals(generatedJobName,
        userConfiguredJobName)) {
      throw new SamzaException(String.format(
          "Generated job.name = %s from app.name = %s does not match user configured job.name = %s, please configure job.name same as app.name",
          generatedJobName, originalConfig.get(ApplicationConfig.APP_NAME), userConfiguredJobName));
    }

    if (generatedJobId != null && userConfiguredJobId != null && !StringUtils.equals(generatedJobId,
        userConfiguredJobId)) {
      throw new SamzaException(String.format(
          "Generated job.id = %s from app.id = %s does not match user configured job.id = %s, please configure job.id same as app.id",
          generatedJobId, originalConfig.get(ApplicationConfig.APP_ID), userConfiguredJobId));
    }
  }

  JobConfig generateJobConfig(JobNode jobNode, String executionPlanJson) {
    if (jobNode.isLegacyTaskApplication()) {
      return new JobConfig(jobNode.getConfig());
    }

    Map<String, String> generatedConfig = new HashMap<>();
    // set up job name and job ID
    generatedConfig.put(JobConfig.JOB_NAME(), jobNode.getJobName());
    generatedConfig.put(JobConfig.JOB_ID(), jobNode.getJobId());

    Map<String, StreamEdge> inEdges = jobNode.getInEdges();
    Map<String, StreamEdge> outEdges = jobNode.getOutEdges();
    Collection<OperatorSpec> reachableOperators = jobNode.getReachableOperators();
    List<StoreDescriptor> stores = getStoreDescriptors(reachableOperators);
    Map<String, TableDescriptor> reachableTables = getReachableTables(reachableOperators, jobNode);

    // config passed by the JobPlanner. user-provided + system-stream descriptor config + misc. other config
    Config originalConfig = jobNode.getConfig();

    // check all inputs to the node for broadcast and input streams
    final Set<String> inputs = new HashSet<>();
    final Set<String> broadcastInputs = new HashSet<>();
    for (StreamEdge inEdge : inEdges.values()) {
      String formattedSystemStream = inEdge.getName();
      if (inEdge.isBroadcast()) {
        if (inEdge.getPartitionCount() > 1) {
          broadcastInputs.add(formattedSystemStream + "#[0-" + (inEdge.getPartitionCount() - 1) + "]");
        } else {
          broadcastInputs.add(formattedSystemStream + "#0");
        }
      } else {
        inputs.add(formattedSystemStream);
      }
    }

    configureBroadcastInputs(generatedConfig, originalConfig, broadcastInputs);

    // compute window and join operator intervals in this node
    configureWindowInterval(generatedConfig, originalConfig, reachableOperators);

    // set store configuration for stateful operators.
    stores.forEach(sd -> generatedConfig.putAll(sd.getStorageConfigs()));

    // set the execution plan in json
    generatedConfig.put(CONFIG_INTERNAL_EXECUTION_PLAN, executionPlanJson);

    // write intermediate input/output streams to configs
    inEdges.values().stream().filter(StreamEdge::isIntermediate)
        .forEach(intermediateEdge -> generatedConfig.putAll(intermediateEdge.generateConfig()));

    // write serialized serde instances and stream, store, and table serdes to configs
    // serde configuration generation has to happen before table configuration, since the serde configuration
    // is required when generating configurations for some TableProvider (i.e. local store backed tables)
    configureSerdes(generatedConfig, inEdges, outEdges, stores, reachableTables.keySet(), jobNode);

    // generate table configuration and potential side input configuration
    configureTables(generatedConfig, originalConfig, reachableTables, inputs);

    // generate the task.inputs configuration
    generatedConfig.put(TaskConfig.INPUT_STREAMS(), Joiner.on(',').join(inputs));

    LOG.info("Job {} has generated configs {}", jobNode.getJobNameAndId(), generatedConfig);

    return new JobConfig(mergeConfig(originalConfig, generatedConfig));
  }

  private Map<String, TableDescriptor> getReachableTables(Collection<OperatorSpec> reachableOperators, JobNode jobNode) {
    // TODO: Fix this in SAMZA-1893. For now, returning all tables for single-job execution plan
    return jobNode.getTables();
  }

  private void configureBroadcastInputs(Map<String, String> configs, Config config, Set<String> broadcastStreams) {
    // TODO: SAMZA-1841: remove this once we support defining broadcast input stream in high-level
    // task.broadcast.input should be generated by the planner in the future.
    if (broadcastStreams.isEmpty()) {
      return;
    }
    String broadcastInputs = config.get(TaskConfigJava.BROADCAST_INPUT_STREAMS);
    if (StringUtils.isNotBlank(broadcastInputs)) {
      broadcastStreams.add(broadcastInputs);
    }
    configs.put(TaskConfigJava.BROADCAST_INPUT_STREAMS, Joiner.on(',').join(broadcastStreams));
  }

  private void configureWindowInterval(Map<String, String> configs, Config config,
      Collection<OperatorSpec> reachableOperators) {
    if (!reachableOperators.stream().anyMatch(op -> op.getOpCode() == OperatorSpec.OpCode.WINDOW
        || op.getOpCode() == OperatorSpec.OpCode.JOIN)) {
      return;
    }

    // set triggering interval if a window or join is defined. Only applies to high-level applications
    long triggerInterval = computeTriggerInterval(reachableOperators);
    LOG.info("Using triggering interval: {}", triggerInterval);

    configs.put(TaskConfig.WINDOW_MS(), String.valueOf(triggerInterval));
  }

  /**
   * Computes the triggering interval to use during the execution of this {@link JobNode}
   */
  private long computeTriggerInterval(Collection<OperatorSpec> reachableOperators) {
    List<Long> windowTimerIntervals =  reachableOperators.stream()
        .filter(spec -> spec.getOpCode() == OperatorSpec.OpCode.WINDOW)
        .map(spec -> ((WindowOperatorSpec) spec).getDefaultTriggerMs())
        .collect(Collectors.toList());

    // Filter out the join operators, and obtain a list of their ttl values
    List<Long> joinTtlIntervals = reachableOperators.stream()
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

  private List<StoreDescriptor> getStoreDescriptors(Collection<OperatorSpec> reachableOperators) {
    return reachableOperators.stream().filter(operatorSpec -> operatorSpec instanceof StatefulOperatorSpec)
        .map(operatorSpec -> ((StatefulOperatorSpec) operatorSpec).getStoreDescriptors()).flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  private void configureTables(Map<String, String> generatedConfig, Config originalConfig,
      Map<String, TableDescriptor> tables, Set<String> inputs) {
    generatedConfig.putAll(
        TableConfigGenerator.generate(
            new MapConfig(generatedConfig), new ArrayList<>(tables.values())));

    // Add side inputs to the inputs and mark the stream as bootstrap
    tables.values().forEach(tableDescriptor -> {
        if (tableDescriptor instanceof LocalTableDescriptor) {
          LocalTableDescriptor localTableDescriptor = (LocalTableDescriptor) tableDescriptor;
          List<String> sideInputs = localTableDescriptor.getSideInputs();
          if (sideInputs != null && !sideInputs.isEmpty()) {
            sideInputs.stream()
                .map(sideInput -> StreamUtil.getSystemStreamFromNameOrId(originalConfig, sideInput))
                .forEach(systemStream -> {
                    inputs.add(StreamUtil.getNameFromSystemStream(systemStream));
                    generatedConfig.put(String.format(StreamConfig.STREAM_PREFIX() + StreamConfig.BOOTSTRAP(),
                        systemStream.getSystem(), systemStream.getStream()), "true");
                  });
          }
        }
      });
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
  private void configureSerdes(Map<String, String> configs, Map<String, StreamEdge> inEdges, Map<String, StreamEdge> outEdges,
      List<StoreDescriptor> stores, Collection<String> tables, JobNode jobNode) {
    // collect all key and msg serde instances for streams
    Map<String, Serde> streamKeySerdes = new HashMap<>();
    Map<String, Serde> streamMsgSerdes = new HashMap<>();
    inEdges.keySet().forEach(streamId ->
        addSerdes(jobNode.getInputSerdes(streamId), streamId, streamKeySerdes, streamMsgSerdes));
    outEdges.keySet().forEach(streamId ->
        addSerdes(jobNode.getOutputSerde(streamId), streamId, streamKeySerdes, streamMsgSerdes));

    Map<String, Serde> storeKeySerdes = new HashMap<>();
    Map<String, Serde> storeMsgSerdes = new HashMap<>();
    stores.forEach(storeDescriptor -> {
        storeKeySerdes.put(storeDescriptor.getStoreName(), storeDescriptor.getKeySerde());
        storeMsgSerdes.put(storeDescriptor.getStoreName(), storeDescriptor.getMsgSerde());
      });

    Map<String, Serde> tableKeySerdes = new HashMap<>();
    Map<String, Serde> tableMsgSerdes = new HashMap<>();
    tables.forEach(tableId -> {
        addSerdes(jobNode.getTableSerdes(tableId), tableId, tableKeySerdes, tableMsgSerdes);
      });

    // for each unique stream or store serde instance, generate a unique name and serialize to config
    HashSet<Serde> serdes = new HashSet<>(streamKeySerdes.values());
    serdes.addAll(streamMsgSerdes.values());
    serdes.addAll(storeKeySerdes.values());
    serdes.addAll(storeMsgSerdes.values());
    serdes.addAll(tableKeySerdes.values());
    serdes.addAll(tableMsgSerdes.values());
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

    // set key and msg serdes for stores to the serde names generated above
    tableKeySerdes.forEach((tableId, serde) -> {
        String keySerdeConfigKey = String.format(JavaTableConfig.STORE_KEY_SERDE, tableId);
        configs.put(keySerdeConfigKey, serdeUUIDs.get(serde));
      });

    tableMsgSerdes.forEach((tableId, serde) -> {
        String valueSerdeConfigKey = String.format(JavaTableConfig.STORE_MSG_SERDE, tableId);
        configs.put(valueSerdeConfigKey, serdeUUIDs.get(serde));
      });
  }

  private void addSerdes(KV<Serde, Serde> serdes, String streamId, Map<String, Serde> keySerdeMap,
      Map<String, Serde> msgSerdeMap) {
    if (serdes != null) {
      if (serdes.getKey() != null && !(serdes.getKey() instanceof NoOpSerde)) {
        keySerdeMap.put(streamId, serdes.getKey());
      }
      if (serdes.getValue() != null && !(serdes.getValue() instanceof NoOpSerde)) {
        msgSerdeMap.put(streamId, serdes.getValue());
      }
    }
  }
}
