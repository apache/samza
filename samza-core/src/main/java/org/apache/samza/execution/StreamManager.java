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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.*;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import static org.apache.samza.util.ScalaToJavaUtils.defaultValue;

public class StreamManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamManager.class);

  private final SystemAdmins systemAdmins;

  public StreamManager(SystemAdmins systemAdmins) {
    this.systemAdmins = systemAdmins;
  }

  public void createStreams(List<StreamSpec> streams) {
    Multimap<String, StreamSpec> streamsGroupedBySystem = HashMultimap.create();
    streams.forEach(streamSpec ->
      streamsGroupedBySystem.put(streamSpec.getSystemName(), streamSpec));

    for (Map.Entry<String, Collection<StreamSpec>> entry : streamsGroupedBySystem.asMap().entrySet()) {
      String systemName = entry.getKey();
      SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(systemName);

      for (StreamSpec stream : entry.getValue()) {
        LOGGER.info("Creating stream {} with partitions {} on system {}",
            new Object[]{stream.getPhysicalName(), stream.getPartitionCount(), systemName});
        systemAdmin.createStream(stream);
      }
    }
  }

  Map<String, Integer> getStreamPartitionCounts(String systemName, Set<String> streamNames) {
    Map<String, Integer> streamToPartitionCount = new HashMap<>();

    SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(systemName);
    if (systemAdmin == null) {
      throw new SamzaException(String.format("System %s does not exist.", systemName));
    }

    // retrieve the metadata for the streams in this system
    Map<String, SystemStreamMetadata> streamToMetadata = systemAdmin.getSystemStreamMetadata(streamNames);
    // set the partitions of a stream to its StreamEdge
    streamToMetadata.forEach((stream, data) ->
      streamToPartitionCount.put(stream, data.getSystemStreamPartitionMetadata().size()));

    return streamToPartitionCount;
  }

  /**
   * This is a best-effort approach to clear the internal streams from previous run, including intermediate streams,
   * checkpoint stream and changelog streams.
   * For batch processing, we always clean up the previous internal streams and create a new set for each run.
   * @param prevConfig config of the previous run
   */
  public void clearStreamsFromPreviousRun(Config prevConfig) {
    try {
      ApplicationConfig appConfig = new ApplicationConfig(prevConfig);
      LOGGER.info("run.id from previous run is {}", appConfig.getRunId());

      StreamConfig streamConfig = new StreamConfig(prevConfig);

      //Find all intermediate streams and clean up
      Set<StreamSpec> intStreams = JavaConversions.asJavaCollection(streamConfig.getStreamIds()).stream()
          .filter(streamConfig::getIsIntermediate)
          .map(id -> new StreamSpec(id, streamConfig.getPhysicalName(id), streamConfig.getSystem(id)))
          .collect(Collectors.toSet());
      intStreams.forEach(stream -> {
          LOGGER.info("Clear intermediate stream {} in system {}", stream.getPhysicalName(), stream.getSystemName());
          systemAdmins.getSystemAdmin(stream.getSystemName()).clearStream(stream);
        });

      //Find checkpoint stream and clean up
      TaskConfig taskConfig = new TaskConfig(prevConfig);
      String checkpointManagerFactoryClass = taskConfig.getCheckpointManagerFactory().getOrElse(defaultValue(null));
      if (checkpointManagerFactoryClass != null) {
        CheckpointManager checkpointManager = ((CheckpointManagerFactory) Util.getObj(checkpointManagerFactoryClass))
            .getCheckpointManager(prevConfig, new MetricsRegistryMap());
        checkpointManager.clearCheckpoints();
      }

      //Find changelog streams and remove them
      StorageConfig storageConfig = new StorageConfig(prevConfig);
      for (String store : JavaConversions.asJavaCollection(storageConfig.getStoreNames())) {
        String changelog = storageConfig.getChangelogStream(store).getOrElse(defaultValue(null));
        if (changelog != null) {
          LOGGER.info("Clear store {} changelog {}", store, changelog);
          SystemStream systemStream = Util.getSystemStreamFromNames(changelog);
          StreamSpec spec = StreamSpec.createChangeLogStreamSpec(systemStream.getStream(), systemStream.getSystem(), 1);
          systemAdmins.getSystemAdmin(spec.getSystemName()).clearStream(spec);
        }
      }
    } catch (Exception e) {
      // For batch, we always create a new set of internal streams (checkpoint, changelog and intermediate) with unique
      // id. So if clearStream doesn't work, it won't affect the correctness of the results.
      // We log a warning here and rely on retention to clean up the streams later.
      LOGGER.warn("Fail to clear internal streams from previous run. Please clean up manually.", e);
    }
  }

  /**
   * Create a unique stream name if it's batch mode and has a valid run.id.
   * @param stream physical name of the stream
   * @param config {@link Config} object
   * @return stream name created
   */
  public static String createUniqueNameForBatch(String stream, Config config) {
    ApplicationConfig appConfig = new ApplicationConfig(config);
    if (appConfig.getAppMode() == ApplicationConfig.ApplicationMode.BATCH && appConfig.getRunId() != null) {
      return stream + "-" + appConfig.getRunId();
    } else {
      return stream;
    }
  }
}
