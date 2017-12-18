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

package org.apache.samza.system.kafka;

import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.MapConfig;
import org.junit.Test;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestKafkaSystemFactoryJava {

  @Test
  public void testGetIntermediateStreamProperties() {
    Map<String, String> config = new HashMap<>();
    KafkaSystemFactory factory = new KafkaSystemFactory();
    Map<String, Properties> properties = JavaConversions.mapAsJavaMap(
        factory.getIntermediateStreamProperties(new MapConfig(config)));
    assertTrue(properties.isEmpty());

    // no properties for stream
    config.put("streams.test.samza.intermediate", "true");
    config.put("streams.test.compression.type", "lz4"); //some random config
    properties = JavaConversions.mapAsJavaMap(
        factory.getIntermediateStreamProperties(new MapConfig(config)));
    assertTrue(properties.isEmpty());

    config.put(ApplicationConfig.APP_MODE, ApplicationConfig.ApplicationMode.BATCH.name());
    properties = JavaConversions.mapAsJavaMap(
        factory.getIntermediateStreamProperties(new MapConfig(config)));
    assertTrue(!properties.isEmpty());
    Properties prop = properties.get("test");
    assertEquals(prop.getProperty("retention.ms"), String.valueOf(KafkaConfig.DEFAULT_RETENTION_MS_FOR_BATCH()));
    assertEquals(prop.getProperty("compression.type"), "lz4");
  }
}
