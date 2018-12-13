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
package org.apache.samza.system.kinesis.descriptors;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.operators.KV;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.kinesis.KinesisConfig;
import org.junit.Assert;
import org.junit.Test;


public class TestKinesisInputDescriptor {
  @Test
  public void testConfigGeneration() {
    String systemName = "kinesis";
    String streamName = "Seine";
    KinesisSystemDescriptor sd = new KinesisSystemDescriptor(systemName);
    Map<String, String> cliConfig = new HashMap<>();
    cliConfig.put("key1", "value1");
    KinesisInputDescriptor<KV<String, byte[]>> id = sd.getInputDescriptor(streamName, new NoOpSerde<byte[]>())
        .withRegion("Paris")
        .withAccessKey("accessKey")
        .withSecretKey("secretKey")
        .withKCLConfig(cliConfig);

    Map<String, String> generatedConfig = id.toConfig();
    Assert.assertEquals(5, generatedConfig.size());

    Assert.assertEquals(systemName, generatedConfig.get("streams.Seine.samza.system"));
    Assert.assertEquals("Paris",
        generatedConfig.get(String.format(KinesisConfig.CONFIG_STREAM_REGION, systemName, streamName)));
    Assert.assertEquals("accessKey",
        generatedConfig.get(String.format(KinesisConfig.CONFIG_STREAM_ACCESS_KEY, systemName, streamName)));
    Assert.assertEquals("secretKey",
        generatedConfig.get(String.format(KinesisConfig.CONFIG_STREAM_SECRET_KEY, systemName, streamName)));
    Assert.assertEquals("value1", generatedConfig.get(
        String.format(KinesisConfig.CONFIG_STREAM_KINESIS_CLIENT_LIB_CONFIG, systemName, streamName) + "key1"));
  }
}
