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

package org.apache.samza.test.framework.stream;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.InMemorySystemConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.test.framework.system.InMemorySystemDescriptor;


/**
 * A descriptor for an in memory stream of messages that can either have single or multiple partitions.
 * <p>
 *  An instance of this descriptor may be obtained from an appropriately configured {@link InMemorySystemDescriptor}.
 * <p>
 * @param <StreamMessageType>
 *        can represent a message with null key or a KV {@link org.apache.samza.operators.KV}, key of which represents key of a
 *        {@link org.apache.samza.system.IncomingMessageEnvelope} or {@link org.apache.samza.system.OutgoingMessageEnvelope}
 *        and value represents the message of the same
 */
public class InMemoryInputDescriptor<StreamMessageType>
    extends InputDescriptor<StreamMessageType, InMemoryInputDescriptor<StreamMessageType>> {
  /**
   * Constructs a new InMemoryInputDescriptor from specified components.
   * @param systemDescriptor represents name of the system stream is associated with
   * @param streamId represents name of the stream
   */
  public InMemoryInputDescriptor(String streamId, InMemorySystemDescriptor systemDescriptor) {
    super(streamId, new NoOpSerde<>(), systemDescriptor, null);
    Preconditions.checkNotNull(systemDescriptor);
    Preconditions.checkNotNull(streamId);
  }

  /**
   * Using this method on an InMemoryInputDescriptor creates a single partitioned inmemory stream
   * and initializes the stream with messages
   *
   * @param partitionData messages used to initialize the single partition stream
   * @return this input descriptor
   */
  public InMemoryInputDescriptor<StreamMessageType> withData(
      List<StreamMessageType> partitionData) {
    Preconditions.checkNotNull(partitionData);
    Map<Integer, Iterable<StreamMessageType>> partition = new HashMap<>();
    partition.put(0, partitionData);
    initializeInMemoryInputStream(partition);
    return this;
  }

  /**
   * Using this method on an InMemoryInputDescriptor creates a multi-partitioned inmemory stream
   * and initializes partitions with stream with messages
   *
   * @param partitionData key of the map represents partitionId and value represents
   *                      messages in the partition
   * @return this input descriptor
   */
  public InMemoryInputDescriptor<StreamMessageType> withData(
      Map<Integer, ? extends Iterable<StreamMessageType>> partitionData) {
    Preconditions.checkNotNull(partitionData);
    initializeInMemoryInputStream(partitionData);
    return this;
  }

  @Override
  public Map<String, String> toConfig() {
    HashMap<String, String> configs = new HashMap<>(super.toConfig());
    InMemorySystemDescriptor descriptor = (InMemorySystemDescriptor) getSystemDescriptor();
    configs.put(InMemorySystemConfig.INMEMORY_SCOPE, descriptor.getInMemoryScope());
    return configs;
  }

  /**
   * Creates an in memory stream with {@link InMemorySystemFactory} and initializes the metadata for the stream.
   * Initializes each partition of that stream with messages
   * @param partitions represents the descriptor describing a stream to initialize with the in memory system
   */
  private void initializeInMemoryInputStream(Map<Integer, ? extends Iterable<StreamMessageType>> partitions) {
    String systemName = getSystemName();
    String physicalName = (String) getPhysicalName().orElse(getStreamId());
    StreamSpec spec = new StreamSpec(getStreamId(), physicalName, systemName, partitions.size());
    SystemFactory factory = new InMemorySystemFactory();
    factory.getAdmin(systemName, new MapConfig(toConfig())).createStream(spec);
    SystemProducer producer = factory.getProducer(systemName, new MapConfig(toConfig()), null);
    SystemStream sysStream = new SystemStream(systemName, physicalName);
    partitions.forEach((partitionId, partition) -> {
        partition.forEach(e -> {
            Object key = e instanceof KV ? ((KV) e).getKey() : null;
            Object value = e instanceof KV ? ((KV) e).getValue() : e;
            producer.send(systemName, new OutgoingMessageEnvelope(sysStream, Integer.valueOf(partitionId), key, value));
          });
        producer.send(systemName, new OutgoingMessageEnvelope(sysStream, Integer.valueOf(partitionId), null, new EndOfStreamMessage(null)));
      });
  }

}
