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

import com.google.common.primitives.Bytes;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.commons.lang3.ArrayUtils;
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

  private static final int VALUE_SEGMENT_SIZE_IN_BYTES = 1020 * 1020;

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

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    zkClient.waitUntilConnected(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] get(String key) {
    byte[] combinedValue = null;
    try {
      for (int segmentIndex = 0;; ++segmentIndex) {
        String zkPath = getZkPathForKey(key, segmentIndex);
        byte[] valueSegment = zkClient.readData(zkPath, true);
        if (valueSegment == null) {
          break;
        } else {
          if (combinedValue == null) {
            combinedValue = valueSegment;
          } else {
            combinedValue = Bytes.concat(combinedValue, valueSegment);
          }
        }
      }
      return combinedValue;
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception occurred when reading the key: %s from zookeeper.", key), e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(String key, byte[] value) {
    try {
      List<byte[]> valueSegments = chunkMetadataStoreValue(value);
      for (int segmentIndex = 0; segmentIndex < valueSegments.size(); segmentIndex++) {
        String zkPath = getZkPathForKey(key, segmentIndex);
        zkClient.createPersistent(zkPath, true);
        zkClient.writeData(zkPath, valueSegments.get(segmentIndex));
      }
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception occurred when storing the key: %s, value: %s from zookeeper.", key, value), e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(String key) {
    String zkPath = String.format("%s/%s", zkBaseDir, key);
    zkClient.deleteRecursive(zkPath);
  }

  /**
   * {@inheritDoc}
   * @throws SamzaException if there're exceptions reading data from zookeeper.
   */
  @Override
  public Map<String, byte[]> all() {
    try {
      List<String> zkSubDirectories = zkClient.getChildren(zkBaseDir);
      Map<String, byte[]> result = new HashMap<>();
      for (String zkSubDir : zkSubDirectories) {
        byte[] value = get(zkSubDir);
        if (value != null) {
          result.put(zkSubDir, value);
        }
      }
      return result;
    } catch (Exception e) {
      String errorMsg = String.format("Error reading path: %s from zookeeper.", zkBaseDir);
      LOG.error(errorMsg, e);
      throw new SamzaException(errorMsg, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() {
    // No-op for zookeeper implementation.
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    zkClient.close();
  }

  private String getZkPathForKey(String key, int valueSegmentIndex) {
    return String.format("%s/%s/%d", zkBaseDir, key, valueSegmentIndex);
  }

  /**
   * Splits the input byte array into independent byte array segments of 1 MB size.
   * @param valueAsBytes the byteArray to split.
   * @return the job model splitted into independent byte array chunks.
   */
  private static List<byte[]> chunkMetadataStoreValue(byte[] valueAsBytes) {
    try {
      List<byte[]> valueSegments = new ArrayList<>();
      int valueLength = valueAsBytes.length;
      for (int index = 0; index < valueLength; index += VALUE_SEGMENT_SIZE_IN_BYTES) {
        byte[] valueSegment = ArrayUtils.subarray(valueAsBytes, index, Math.min(index + VALUE_SEGMENT_SIZE_IN_BYTES, valueLength));
        valueSegments.add(valueSegment);
      }
      return valueSegments;
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception occurred when splitting the value: %s to small chunks.", valueAsBytes), e);
    }
  }
}
