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

package org.apache.samza.system.kinesis;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

import static org.junit.Assert.*;


public class TestKinesisConfig {
  @Test
  public void testGetKinesisStreams() {
    Map<String, String> kv = new HashMap<>();
    kv.put("systems.kinesis.streams.kinesis-stream1.prop1", "value1");
    kv.put("systems.kinesis.streams.kinesis-stream1.prop2", "value2");
    kv.put("systems.kinesis.streams.kinesis-stream2.prop1", "value3");

    Config config = new MapConfig(kv);
    KinesisConfig kConfig = new KinesisConfig(config);

    Set<String> streams = kConfig.getKinesisStreams("kinesis");
    assertEquals(2, streams.size());
  }

  @Test
  public void testKinesisConfigs() {
    Map<String, String> kv = new HashMap<>();
    String system = "kinesis";
    String stream = "kinesis-stream";
    String systemConfigPrefix = String.format("systems.%s.", system);
    String ssConfigPrefix = String.format("systems.%s.streams.%s.", system, stream);

    kv.put("sensitive." + ssConfigPrefix + "aws.secretKey", "secretKey");
    kv.put(systemConfigPrefix + "aws.region", "us-east-1");
    kv.put(ssConfigPrefix + "aws.accessKey", "accessKey");

    Config config = new MapConfig(kv);
    KinesisConfig kConfig = new KinesisConfig(config);

    assertEquals("us-east-1", kConfig.getRegion(system, stream).getName());
    assertEquals("accessKey", kConfig.getStreamAccessKey(system, stream));
    assertEquals("secretKey", kConfig.getStreamSecretKey(system, stream));
  }

  @Test
  public void testAwsClientConfigs() {
    Map<String, String> kv = new HashMap<>();
    String system = "kinesis";
    String systemConfigPrefix = String.format("systems.%s.", system);

    // Aws Client Configs
    kv.put(systemConfigPrefix + "aws.clientConfig.ProxyHost", "hostName");
    kv.put(systemConfigPrefix + "aws.clientConfig.ProxyPort", "8080");

    Config config = new MapConfig(kv);
    KinesisConfig kConfig = new KinesisConfig(config);

    assertEquals("hostName", kConfig.getAWSClientConfig(system).getProxyHost());
    assertEquals(8080, kConfig.getAWSClientConfig(system).getProxyPort());
  }

  @Test
  public void testKclConfigs() {
    Map<String, String> kv = new HashMap<>();
    String system = "kinesis";
    String stream = "kinesis-stream";
    String systemConfigPrefix = String.format("systems.%s.", system);

    // region config is required for setting kcl config.
    kv.put(systemConfigPrefix + "aws.region", "us-east-1");

    // Kcl Configs
    kv.put(systemConfigPrefix + "aws.kcl.TableName", "sample-table");
    kv.put(systemConfigPrefix + "aws.kcl.MaxRecords", "100");
    kv.put(systemConfigPrefix + "aws.kcl.CallProcessRecordsEvenForEmptyRecordList", "true");
    kv.put(systemConfigPrefix + "aws.kcl.InitialPositionInStream", "TRIM_HORIZON");
    // override one of the Kcl configs for kinesis-stream1
    kv.put(systemConfigPrefix + "streams.kinesis-stream1.aws.kcl.InitialPositionInStream", "LATEST");

    Config config = new MapConfig(kv);
    KinesisConfig kConfig = new KinesisConfig(config);
    KinesisClientLibConfiguration kclConfig = kConfig.getKinesisClientLibConfig(system, stream, "sample-app");

    assertEquals("sample-table", kclConfig.getTableName());
    assertEquals(100, kclConfig.getMaxRecords());
    assertTrue(kclConfig.shouldCallProcessRecordsEvenForEmptyRecordList());
    assertEquals(InitialPositionInStream.TRIM_HORIZON, kclConfig.getInitialPositionInStream());

    // verify if the overriden config is applied for kinesis-stream1
    kclConfig = kConfig.getKinesisClientLibConfig(system, "kinesis-stream1", "sample-app");
    assertEquals(InitialPositionInStream.LATEST, kclConfig.getInitialPositionInStream());
  }

  @Test
  public void testgetKCLConfigWithUnknownConfigs() {
    Map<String, String> kv = new HashMap<>();
    kv.put("systems.kinesis.aws.region", "us-east-1");
    kv.put("systems.kinesis.streams.kinesis-stream.aws.kcl.random", "value");

    Config config = new MapConfig(kv);
    KinesisConfig kConfig = new KinesisConfig(config);

    // Should not throw any exception and just ignore the unknown configs.
    kConfig.getKinesisClientLibConfig("kinesis", "kinesis-stream", "sample-app");
  }
}
