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
import com.google.common.primitives.Longs;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import org.apache.helix.zookeeper.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.SamzaException;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link MetadataStore} interface where the
 * metadata of the Samza job is stored in zookeeper.
 */
public class ZkMetadataStore implements MetadataStore {

  private static final Logger LOG = LoggerFactory.getLogger(ZkMetadataStore.class);

  private static final int CHECKSUM_SIZE_IN_BYTES = 8;

  /**
   * By default, the maximum data node size supported by zookeeper server is 1 MB.
   * ZkClient prepends any value with serialization bytes before storing to zookeeper server.
   * Maximum segment size constant is initialized after factoring in size of serialization bytes.
   */
  private static final int VALUE_SEGMENT_SIZE_IN_BYTES = 1020 * 1020;

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
    byte[] aggregatedZNodeValues = new byte[0];
    for (int segmentIndex = 0;; ++segmentIndex) {
      String zkPath = getZkPath(key, segmentIndex);
      byte[] zNodeValue = zkClient.readData(zkPath, true);
      if (zNodeValue == null) {
        break;
      }
      aggregatedZNodeValues = Bytes.concat(aggregatedZNodeValues, zNodeValue);
    }
    if (aggregatedZNodeValues.length > 0) {
      byte[] value = ArrayUtils.subarray(aggregatedZNodeValues, 0, aggregatedZNodeValues.length - CHECKSUM_SIZE_IN_BYTES);
      byte[] checkSum = ArrayUtils.subarray(aggregatedZNodeValues, aggregatedZNodeValues.length - CHECKSUM_SIZE_IN_BYTES, aggregatedZNodeValues.length);
      byte[] expectedChecksum = getCRCChecksum(value);
      if (!Arrays.equals(checkSum, expectedChecksum)) {
        String exceptionMessage = String.format("Expected checksum: %s did not match the actual checksum: %s for value: %s",
            Arrays.toString(expectedChecksum), Arrays.toString(value), Arrays.toString(checkSum));
        LOG.error(exceptionMessage);
        throw new IllegalStateException(exceptionMessage);
      }
      return value;
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(String key, byte[] value) {
    List<byte[]> valueSegments = chunkMetadataStoreValue(value);
    for (int segmentIndex = 0; segmentIndex < valueSegments.size(); segmentIndex++) {
      String zkPath = getZkPath(key, segmentIndex);
      zkClient.createPersistent(zkPath, true);
      zkClient.writeData(zkPath, valueSegments.get(segmentIndex));
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
    List<String> zkSubDirectories = zkClient.getChildren(zkBaseDir);
    Map<String, byte[]> result = new HashMap<>();
    for (String zkSubDir : zkSubDirectories) {
      byte[] value = get(zkSubDir);
      if (value != null) {
        result.put(zkSubDir, value);
      }
    }
    return result;
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

  private String getZkPath(String key, int segmentIndex) {
    return String.format("%s/%s/%d", zkBaseDir, key, segmentIndex);
  }

  /**
   * Computes and returns the crc32 checksum of the input byte array.
   * @param value the input byte array.
   * @return the crc32 checksum of the byte array.
   */
  private static byte[] getCRCChecksum(byte[] value) {
    CRC32 crc32 = new CRC32();
    crc32.update(value);
    long checksum = crc32.getValue();
    return Longs.toByteArray(checksum);
  }

  /**
   * Splits the input byte array value into independent byte array segments of 1 MB size.
   * @param value the input byte array to split.
   * @return the byte array splitted into independent byte array chunks.
   */
  private static List<byte[]> chunkMetadataStoreValue(byte[] value) {
    try {
      byte[] checksum = getCRCChecksum(value);
      byte[] valueWithChecksum = ArrayUtils.addAll(value, checksum);
      List<byte[]> valueSegments = new ArrayList<>();
      int length = valueWithChecksum.length;
      for (int index = 0; index < length; index += VALUE_SEGMENT_SIZE_IN_BYTES) {
        byte[] valueSegment = ArrayUtils.subarray(valueWithChecksum, index, Math.min(index + VALUE_SEGMENT_SIZE_IN_BYTES, length));
        valueSegments.add(valueSegment);
      }
      return valueSegments;
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception occurred when splitting the value: %s to small chunks.", value), e);
    }
  }
}
