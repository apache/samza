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

package org.apache.samza.test.framework.system;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.inmemory.InMemorySystemFactory;


/**
 * CollectionStreamSystem represents a system that interacts with an underlying {@link InMemorySystemFactory} to create
 * various input and output streams and initialize {@link org.apache.samza.system.SystemStreamPartition} with messages
 * <p>
 * Following system level configs are set by default
 * <ol>
 *   <li>"systems.%s.default.stream.samza.offset.default" = "oldest"</li>
 *   <li>"systems.%s.samza.factory" = {@link InMemorySystemFactory}</li>
 * </ol>
 * 
 */
public class CollectionStreamSystem {
  private static final String SYSTEM_FACTORY = "systems.%s.samza.factory";
  private static final String SYSTEM_OFFSET = "systems.%s.default.stream.samza.offset.default";

  private String systemName;
  private Map<String, String> systemConfigs;
  private InMemorySystemFactory factory;

  /**
   * Constructs a new CollectionStreamSystem from specified components.
   * <p>
   * Every {@link CollectionStreamSystem} is assumed to consume from the oldest offset, since stream is in memory and
   * is used for testing purpose. System uses {@link InMemorySystemFactory} to initialize in memory streams.
   * <p>
   * @param systemName represents unique name of the system
   */
  private CollectionStreamSystem(String systemName) {
    this.systemName = systemName;
    factory = new InMemorySystemFactory();
    systemConfigs = new HashMap<String, String>();
    systemConfigs.put(String.format(SYSTEM_FACTORY, systemName), InMemorySystemFactory.class.getName());
    systemConfigs.put(String.format(SYSTEM_OFFSET, systemName), "oldest");
  }

  public String getSystemName() {
    return systemName;
  }

  public InMemorySystemFactory getFactory() {
    return factory;
  }

  public Map<String, String> getSystemConfigs() {
    return systemConfigs;
  }

  /**
   * Creates a {@link CollectionStreamSystem} with name {@code name}
   * @param name represents name of the {@link CollectionStreamSystem}
   * @return an instance of {@link CollectionStreamSystem}
   */
  public static CollectionStreamSystem create(String name) {
    Preconditions.checkState(name != null);
    return new CollectionStreamSystem(name);
  }

  /**
   * Creates an in memory stream with {@link InMemorySystemFactory} and initializes the metadata for the stream.
   * Initializes each partition of that stream with messages supplied from {@code partitions}
   *
   * @param streamName represents the name of stream to initialize with the system
   * @param partitions Key of an entry in {@code partitions} represents a {@code partitionId} of a {@link org.apache.samza.Partition}
   *                   and value represents the stream of messages the {@link org.apache.samza.system.SystemStreamPartition}
   *                   will be initialized with
   * @param <T> can represent a message or a KV {@link org.apache.samza.operators.KV}, key of which represents key of a
   *            {@link org.apache.samza.system.IncomingMessageEnvelope} or {@link org.apache.samza.system.OutgoingMessageEnvelope}
   *            and value represents the message
   * @return the current {@link CollectionStreamSystem} with a new stream {@code streamName} that a Samza job can
   *         consume from
   */
  public <T> CollectionStreamSystem addInput(String streamName, Map<Integer, Iterable<T>> partitions) {
    Preconditions.checkState(streamName != null);
    Preconditions.checkState(partitions.size() >= 1);
    StreamSpec spec = new StreamSpec(streamName, streamName, systemName, partitions.size());
    factory.getAdmin(systemName, new MapConfig(systemConfigs)).createStream(spec);
    SystemProducer producer = factory.getProducer(systemName, new MapConfig(systemConfigs), null);
    partitions.forEach((partitionId, partition) -> {
        partition.forEach(e -> {
            Object key = e instanceof KV ? ((KV) e).getKey() : null;
            Object value = e instanceof KV ? ((KV) e).getValue() : e;
            producer.send(systemName,
                new OutgoingMessageEnvelope(new SystemStream(systemName, streamName), Integer.valueOf(partitionId), key,
                    value));
          });
        producer.send(systemName,
            new OutgoingMessageEnvelope(new SystemStream(systemName, streamName), Integer.valueOf(partitionId), null,
                new EndOfStreamMessage(null)));
      });
    return this;
  }

  /**
   * Creates an empty in memory stream with {@link InMemorySystemFactory} and initializes the metadata for the stream.
   * This stream is supposed used by Samza job to produce/write messages to.
   *
   * @param streamName represents the name of the stream
   * @param partitionCount represents the number of partitions in a stream
   * @param <T> represents the type of message
   * @return the current {@link CollectionStreamSystem} with a new stream {@code streamName} that a Samza job can
   *         produce to
   */
  public <T> CollectionStreamSystem addOutput(String streamName, Integer partitionCount) {
    Preconditions.checkNotNull(streamName);
    Preconditions.checkNotNull(systemName);
    Preconditions.checkState(partitionCount >= 1);
    StreamSpec spec = new StreamSpec(streamName, streamName, systemName, partitionCount);
    factory.getAdmin(systemName, new MapConfig(systemConfigs)).createStream(spec);
    return this;
  }
}

