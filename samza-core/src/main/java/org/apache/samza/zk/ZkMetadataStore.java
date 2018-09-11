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
package org.apache.samza.zk;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.samza.config.Config;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.SamzaException;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link MetadataStore} interface where the
 * metadata of the Samza job is stored in zookeeper.
 */
public class ZkMetadataStore implements MetadataStore {

  private static final Logger LOG = LoggerFactory.getLogger(ZkMetadataStore.class);

  private final ZkClient zkClient;
  private final ZkConfig zkConfig;
  private final String zkBaseDir;

  public ZkMetadataStore(String zkBaseDir, Config config, MetricsRegistry metricsRegistry) {
    this.zkConfig = new ZkConfig(config);
    this.zkClient = new ZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs(), new BytesPushThroughSerializer());
    this.zkBaseDir = zkBaseDir;
    zkClient.createPersistent(zkBaseDir, true);
  }

  @Override
  public void init() {
    zkClient.waitUntilConnected(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
  }

  @Override
  public byte[] get(byte[] key) {
    return zkClient.readData(getZkPathForKey(key), true);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    String zkPath = getZkPathForKey(key);
    zkClient.createPersistent(zkPath, true);
    zkClient.writeData(zkPath, value);
  }

  @Override
  public void delete(byte[] key) {
    zkClient.delete(getZkPathForKey(key));
  }

  @Override
  public Map<byte[], byte[]> all() {
    try {
      List<String> zkSubDirectories = zkClient.getChildren(zkBaseDir);
      Map<byte[], byte[]> result = new HashMap<>();
      for (String zkSubDir : zkSubDirectories) {
        String completeZkPath = String.format("%s/%s", zkBaseDir, zkSubDir);
        byte[] value = zkClient.readData(completeZkPath, true);
        if (value != null) {
          result.put(completeZkPath.getBytes("UTF-8"), value);
        }
      }
      return result;
    } catch (Exception e) {
      LOG.error("Exception occurred on reading the path: {} from zookeeper.", zkBaseDir, e);
      throw new SamzaException(e);
    }
  }

  @Override
  public void flush() {
    // No-op for zookeeper implementation.
  }

  @Override
  public void close() {
    zkClient.close();
  }

  private String getZkPathForKey(byte[] key) {
    return String.format("%s/%s", zkBaseDir, new String(key, Charset.forName("UTF-8")));
  }
}
