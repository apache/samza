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

package org.apache.samza.serializers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricsSnapshotSerdeV2 implements Serde<MetricsSnapshot> {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsSnapshotSerdeV2.class);
  private final ObjectMapper jsonMapper;

  public MetricsSnapshotSerdeV2() {
    jsonMapper = new ObjectMapper();
    jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    jsonMapper.setVisibility(JsonMethod.ALL, JsonAutoDetect.Visibility.ANY);
    jsonMapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
    // required to serialize Throwable with type and other info
  }

  @Override
  public MetricsSnapshot fromBytes(byte[] bytes) {
    try {
      return MetricsSnapshot.fromMap(
          jsonMapper.readValue(bytes, new HashMap<String, Map<String, Object>>().getClass()));
    } catch (IOException e) {
      LOG.info("Exception while deserializing", e);
    }
    return null;
  }

  @Override
  public byte[] toBytes(MetricsSnapshot metricsSnapshot) {
    try {
      return jsonMapper.writeValueAsString(convertMap(metricsSnapshot.getAsMap())).getBytes("UTF-8");
    } catch (IOException e) {
      LOG.info("Exception while serializing", e);
    }
    return null;
  }

  /** Unmodifiable maps should not be serialized with type, because UnmodifiableMap cannot be deserialized.
   * So we convert to HashMap. This is a Jackson limitation.
   */
  private HashMap convertMap(Map<String, Object> map) {
    HashMap retVal = new HashMap(map);
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() instanceof Map) {
        retVal.put(entry.getKey(), convertMap((Map<String, Object>) entry.getValue()));
      }
    }
    return retVal;
  }
}