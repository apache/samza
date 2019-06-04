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

import org.apache.samza.system.kinesis.KinesisConfig;
import org.apache.samza.system.kinesis.KinesisSystemFactory;
import org.junit.Assert;
import org.junit.Test;


public class TestKinesisSystemDescriptor {
  @Test
  public void testConfigGeneration() {
    String systemName = "kinesis";
    Map<String, String> kclConfig = new HashMap<>();
    kclConfig.put("key1", "value1");
    Map<String, String> awsConfig = new HashMap<>();
    awsConfig.put("key2", "value2");

    KinesisSystemDescriptor sd = new KinesisSystemDescriptor(systemName).withRegion("London")
        .withProxyHost("US")
        .withProxyPort(1776)
        .withAWSConfig(awsConfig)
        .withKCLConfig(kclConfig);

    Map<String, String> generatedConfig = sd.toConfig();
    Assert.assertEquals(6, generatedConfig.size());

    Assert.assertEquals(KinesisSystemFactory.class.getName(), generatedConfig.get("systems.kinesis.samza.factory"));
    Assert.assertEquals("London", generatedConfig.get(String.format(KinesisConfig.CONFIG_SYSTEM_REGION, systemName)));
    Assert.assertEquals("US", generatedConfig.get(String.format(KinesisConfig.CONFIG_PROXY_HOST, systemName)));
    Assert.assertEquals("1776", generatedConfig.get(String.format(KinesisConfig.CONFIG_PROXY_PORT, systemName)));
    Assert.assertEquals("value1",
        generatedConfig.get(String.format(KinesisConfig.CONFIG_SYSTEM_KINESIS_CLIENT_LIB_CONFIG, systemName) + "key1"));
    Assert.assertEquals("value2",
        generatedConfig.get(String.format(KinesisConfig.CONFIG_AWS_CLIENT_CONFIG, systemName) + "key2"));
  }
}
