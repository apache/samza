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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.StreamConfig$;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;

public class StreamUtil {
  /**
   * Returns a SystemStream object based on the system stream name given. For
   * example, kafka.topic would return new SystemStream("kafka", "topic").
   */
  public static SystemStream getSystemStreamFromNames(String systemStreamNames) {
    int idx = systemStreamNames.indexOf('.');
    if (idx < 0) {
      throw new IllegalArgumentException("No '.' in stream name '" + systemStreamNames +
          "'. Stream names should be in the form 'system.stream'");
    }
    return new SystemStream(
        systemStreamNames.substring(0, idx),
        systemStreamNames.substring(idx + 1, systemStreamNames.length()));
  }

  /**
   * Returns a SystemStream object based on the system stream name given. For
   * example, kafka.topic would return new SystemStream("kafka", "topic").
   */
  public static String getNameFromSystemStream(SystemStream systemStream) {
    return systemStream.getSystem() + "." + systemStream.getStream();
  }

  public static Set<StreamSpec> getStreamSpecs(Set<String> streamIds, StreamConfig streamConfig) {
    return streamIds.stream().map(streamId -> getStreamSpec(streamId, streamConfig)).collect(Collectors.toSet());
  }

  public static StreamSpec getStreamSpec(String streamId, StreamConfig streamConfig) {
    String physicalName = streamConfig.getPhysicalName(streamId);
    String system = streamConfig.getSystem(streamId);
    Map<String, String> streamProperties = streamConfig.getStreamProperties(streamId);
    return new StreamSpec(streamId, physicalName, system, streamProperties);
  }

  /**
   * Converts the provided list of (streamId, system, physicalName) triplets to their corresponding
   * stream.stream-id.* configurations.
   *
   * @param streams a list of (streamId, system, physicalName) triplets to get the stream configuration for.
   * @return the configuration for the provided { @code streams}
   */
  public static Config toStreamConfigs(List<ImmutableTriple<String, String, String>> streams) {
    Map<String, String> configsMap = new HashMap<>();
    streams.stream().forEach(triple -> {
        String streamId = triple.getLeft();
        String systemName = triple.getMiddle();
        String physicalName = triple.getRight();
        configsMap.put(String.format(StreamConfig$.MODULE$.SYSTEM_FOR_STREAM_ID(), streamId), systemName);
        configsMap.put(String.format(StreamConfig$.MODULE$.PHYSICAL_NAME_FOR_STREAM_ID(), streamId), physicalName);
      });
    return new MapConfig(configsMap);
  }
}
