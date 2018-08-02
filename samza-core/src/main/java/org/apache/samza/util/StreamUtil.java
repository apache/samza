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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;

public class StreamUtil {
  /**
   * Gets the {@link SystemStream} corresponding to the provided stream, which may be
   * a streamId, or stream name of the format systemName.streamName.
   *
   * @param config the config for the job
   * @param stream the stream name or id to get the {@link SystemStream} for.
   * @return the {@link SystemStream} for the stream
   */
  public static SystemStream getSystemStreamFromNameOrId(Config config, String stream) {
    String[] parts = stream.split("\\.");
    if (parts.length == 0 || parts.length > 2) {
      throw new SamzaException(
          String.format("Invalid stream %s. Expected to be of the format streamId or systemName.streamName", stream));
    }
    if (parts.length == 1) {
      return new StreamConfig(config).streamIdToSystemStream(stream);
    } else {
      return new SystemStream(parts[0], parts[1]);
    }
  }

  /**
   * Returns a SystemStream object based on the system stream name given. For
   * example, kafka.topic would return SystemStream("kafka", "topic").
   *
   * @param systemStreamName name of the system stream
   * @return the {@link SystemStream} for the {@code systemStreamName}
   */
  public static SystemStream getSystemStreamFromNames(String systemStreamName) {
    int idx = systemStreamName.indexOf('.');
    if (idx < 0) {
      throw new IllegalArgumentException("No '.' in stream name '" + systemStreamName +
          "'. Stream names should be in the form 'system.stream'");
    }
    return new SystemStream(
        systemStreamName.substring(0, idx),
        systemStreamName.substring(idx + 1, systemStreamName.length()));
  }

  /**
   * Returns the period separated system stream name for the provided {@code systemStream}. For
   * example, SystemStream("kafka", "topic") would return "kafka.topic".
   *
   * @param systemStream the {@link SystemStream} to get the name for
   * @return the system stream name
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
}
