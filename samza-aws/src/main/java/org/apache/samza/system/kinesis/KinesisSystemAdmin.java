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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.model.StreamDescription;


/**
 * A Kinesis-based implementation of SystemAdmin.
 */
public class KinesisSystemAdmin implements SystemAdmin {

  private static final SystemStreamMetadata.SystemStreamPartitionMetadata SYSTEM_STREAM_PARTITION_METADATA =
      new SystemStreamMetadata.SystemStreamPartitionMetadata(ExtendedSequenceNumber.TRIM_HORIZON.getSequenceNumber(),
          ExtendedSequenceNumber.LATEST.getSequenceNumber(),
          ExtendedSequenceNumber.LATEST.getSequenceNumber());

  private static final Logger LOG = LoggerFactory.getLogger(KinesisSystemAdmin.class.getName());

  private final String system;
  private final KinesisConfig kConfig;

  public KinesisSystemAdmin(String system, KinesisConfig kConfig) {
    this.system = system;
    this.kConfig = kConfig;
  }

  /**
   * Source of truth for checkpointing is always kinesis and the offsets written to samza checkpoint topic are ignored.
   * Hence, return null for the getOffsetsAfter for a supplied map of ssps.
   */
  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    Map<SystemStreamPartition, String> offsetsAfter = new HashMap<>();

    for (SystemStreamPartition systemStreamPartition : offsets.keySet()) {
      offsetsAfter.put(systemStreamPartition, null);
    }

    return offsetsAfter;
  }

  /**
   * Source of truth for checkpointing is always kinesis and the offsets given by samza are always ignored by KCL.
   * Hence, return a placeholder for each ssp.
   */
  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    return streamNames.stream().collect(Collectors.toMap(Function.identity(), this::createSystemStreamMetadata));
  }

  private SystemStreamMetadata createSystemStreamMetadata(String stream) {
    LOG.info("create stream metadata for stream {} based on aws stream", stream);
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> metadata = new HashMap<>();
    AmazonKinesisClient client = null;

    try {
      ClientConfiguration clientConfig = kConfig.getAWSClientConfig(system);
      AmazonKinesisClientBuilder builder = AmazonKinesisClientBuilder.standard()
          .withCredentials(kConfig.credentialsProviderForStream(system, stream))
          .withClientConfiguration(clientConfig);
      builder.setRegion(kConfig.getRegion(system, stream).getName());
      client = (AmazonKinesisClient) builder.build();
      StreamDescription desc = client.describeStream(stream).getStreamDescription();
      IntStream.range(0, desc.getShards().size())
          .forEach(i -> metadata.put(new Partition(i), SYSTEM_STREAM_PARTITION_METADATA));
    } catch (Exception e) {
      String errMsg = "couldn't load metadata for stream " + stream;
      LOG.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    } finally {
      if (client != null) {
        client.shutdown();
      }
    }

    return new SystemStreamMetadata(stream, metadata);
  }

  /**
   * Checkpoints are written to KCL and is always the source of truth. Format for Samza offsets is different from
   * that of Kinesis checkpoint. Samza offsets are not comparable. Hence, return null.
   */
  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    return null;
  }
}
