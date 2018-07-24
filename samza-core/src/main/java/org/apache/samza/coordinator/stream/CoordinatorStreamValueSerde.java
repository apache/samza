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
package org.apache.samza.coordinator.stream;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.SamzaException;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;

/**
 * Serializer for values written into the coordinator stream(kafka topic). CoordinatorStreamMessage combines both key
 * and value serializer for coordinator stream messages. Since value is relevant to this serializer, coordinator stream
 * key is nuked for different message types.
 */
public class CoordinatorStreamValueSerde implements Serde<String> {

  private static final String SOURCE = "SamzaContainer";

  private final String type;
  private final Serde<Map<String, Object>> messageSerde;
  private final Config config;

  public CoordinatorStreamValueSerde(Config config, String type) {
    Preconditions.checkNotNull(type);
    this.config = config;
    this.type = type;
    messageSerde = new JsonSerde<>();
  }

  @Override
  public String fromBytes(byte[] bytes) {
    Map<String, Object> values = messageSerde.fromBytes(bytes);
    CoordinatorStreamMessage message = new CoordinatorStreamMessage(new Object[]{}, values);
    if (type.equalsIgnoreCase(SetContainerHostMapping.TYPE)) {
      SetContainerHostMapping hostMapping = new SetContainerHostMapping(message);
      return hostMapping.getHostLocality();
    } else if (type.equalsIgnoreCase(SetTaskContainerMapping.TYPE)) {
      SetTaskContainerMapping setTaskContainerMapping = new SetTaskContainerMapping(message);
      return setTaskContainerMapping.getTaskAssignment();
    } else if (type.equalsIgnoreCase(SetChangelogMapping.TYPE)) {
      SetChangelogMapping changelogMapping = new SetChangelogMapping(message);
      return String.valueOf(changelogMapping.getPartition());
    } else {
      throw new SamzaException(String.format("Unknown coordinator stream message type: %s", type));
    }
  }

  @Override
  public byte[] toBytes(String value) {
    if (type.equalsIgnoreCase(SetContainerHostMapping.TYPE)) {
      SetContainerHostMapping hostMapping = new SetContainerHostMapping(SOURCE, "", value, config.get("jmx.tunneling.url"), config.get("jmx.url"));
      return messageSerde.toBytes(hostMapping.getMessageMap());
    } else if (type.equalsIgnoreCase(SetTaskContainerMapping.TYPE)) {
      SetTaskContainerMapping setTaskContainerMapping = new SetTaskContainerMapping(SOURCE, "", value);
      return messageSerde.toBytes(setTaskContainerMapping.getMessageMap());
    } else if (type.equalsIgnoreCase(SetChangelogMapping.TYPE)) {
      SetChangelogMapping changelogMapping = new SetChangelogMapping(SOURCE, "", Integer.valueOf(value));
      return messageSerde.toBytes(changelogMapping.getMessageMap());
    } else {
      throw new SamzaException(String.format("Unknown coordinator stream message type: %s", type));
    }
  }
}
