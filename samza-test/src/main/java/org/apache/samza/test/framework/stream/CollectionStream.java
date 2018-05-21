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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A CollectionStream represents an in memory stream of messages that can either have single or multiple partitions.
 * Every CollectionStream is coupled with a {@link org.apache.samza.test.framework.system.CollectionStreamSystemSpec} that
 * contains all the specification for system
 *<p>
 * When sending messages using {@code CollectionStream<KV<K, V>>}, messages use K as key and V as message
 * When sending messages using {@code CollectionStream<T>}, messages use a nullkey.
 *</p>
 * @param <T>
 *        can represent a message with null key or a KV {@link org.apache.samza.operators.KV}, key of which represents key of a
 *        {@link org.apache.samza.system.IncomingMessageEnvelope} or {@link org.apache.samza.system.OutgoingMessageEnvelope}
 *        and value represents the message of the same
 */
public class CollectionStream<T> {
  private Integer testId;
  private final String streamName;
  private final String systemName;
  private Map<Integer, Iterable<T>> partitions;
  private Map<String, String> streamConfig;
  private static final String STREAM_TO_SYSTEM = "streams.%s.samza.system";


  /**
   * Constructs a new CollectionStream from specified components.
   * @param systemName represents name of the system stream is associated with
   * @param streamName represents name of the stream
   */
  private CollectionStream(String systemName, String streamName) {
    Preconditions.checkNotNull(systemName);
    Preconditions.checkNotNull(streamName);
    this.systemName = systemName;
    this.streamName = streamName;
    this.streamConfig = new HashMap<>();
    streamConfig.put(String.format(STREAM_TO_SYSTEM, this.streamName), systemName);
  }


  /**
   * Constructs a new CollectionStream with multiple empty partitions from specified components.
   * @param systemName represents name of the system stream is associated with
   * @param streamName represents name of the stream
   * @param partitionCount represents number of partitions, each of these partitions will be empty
   */
  private CollectionStream(String systemName, String streamName, Integer partitionCount) {
    this(systemName, streamName);
    Preconditions.checkState(partitionCount > 0);
    partitions = new HashMap<>();
    for (int i = 0; i < partitionCount; i++) {
      partitions.put(i, new ArrayList<>());
    }
  }

  /**
   * Constructs a new CollectionStream with single partition from specified components.
   * @param systemName represents name of the system stream is associated with
   * @param streamName represents name of the stream
   * @param initPartition represents the messages that the stream will be intialized with, default partitionId for the
   *                  this single partition stream is 0
   */
  private CollectionStream(String systemName, String streamName, Iterable<T> initPartition) {
    this(systemName, streamName);
    Preconditions.checkNotNull(initPartition);
    partitions = new HashMap<>();
    partitions.put(0, initPartition);
  }

  /**
   * Constructs a new CollectionStream with multiple partitions from specified components.
   * @param systemName represents name of the system stream is associated with
   * @param streamName represents name of the stream
   * @param initPartitions represents the partition state, key of the map represents partitionId and value represents
   *                   the messages that partition will be initialized with
   */
  private CollectionStream(String systemName, String streamName, Map<Integer, ? extends Iterable<T>> initPartitions) {
    this(systemName, streamName);
    Preconditions.checkNotNull(initPartitions);
    partitions = new HashMap<>(initPartitions);
  }

  /**
   * @return The Map of partitions that input stream is supposed to be initialized with, this method is
   * used internally and should not be used for asserting over streams.
   * The true state of stream is determined by {@code consmeStream()} of {@link org.apache.samza.test.framework.TestRunner}
   */
  public Map<Integer, Iterable<T>> getInitPartitions() {
    return partitions;
  }

  public String getStreamName() {
    return streamName;
  }

  public String getSystemName() {
    return systemName;
  }

  public Map<String, String> getStreamConfig() {
    return streamConfig;
  }

  public Integer getTestId() {
    return testId;
  }

  public void setTestId(Integer testId) {
    this.testId = testId;
  }

  /**
   * Creates an in memory stream with the name {@code streamName} and initializes the stream to only one partition
   *
   * @param systemName represents name of the system stream is associated with
   * @param streamName represents the name of the Stream
   * @param <T> represents the type of each message in a stream
   * @return an {@link CollectionStream} with only one partition that can contain messages of the type
   */
  public static <T> CollectionStream<T> empty(String systemName, String streamName) {
    return new CollectionStream<>(systemName, streamName, 1);
  }

  /**
   * Creates an in memory stream with the name {@code streamName} and initializes the stream to have as many partitions
   * as specifed by {@code partitionCount}. These partitions are empty and are supposed to be used by Samza job to produce
   * messages to.
   *
   * @param systemName represents name of the system stream is associated with
   * @param streamName represents the name of the Stream
   * @param partitionCount represents the number of partitions the stream would have
   * @param <T> represents the type of each message in a stream
   * @return an empty {@link CollectionStream} with multiple partitions that can contain messages of the type {@code T}
   */
  public static <T> CollectionStream<T> empty(String systemName, String streamName, int partitionCount) {
    return new CollectionStream<>(systemName, streamName, partitionCount);
  }

  /**
   * Creates an in memory stream with the name {@code streamName}. Stream is created with single partition having
   * {@code partitionId} is 0. This partition is intialzied with messages of type T
   *
   * @param systemName represents name of the system stream is associated with
   * @param streamName represents the name of the Stream
   * @param partition represents the messages that the {@link org.apache.samza.system.SystemStreamPartition} will be
   *                  initialized with
   * @param <T> represents the type of a message in the stream
   * @return a {@link CollectionStream} with only one partition containing messages of the type {@code T}
   *
   */
  public static <T> CollectionStream<T> of(String systemName, String streamName, Iterable<T> partition) {
    return new CollectionStream<>(systemName, streamName, partition);
  }

  /**
   * Creates an in memory stream with the name {@code streamName} and initializes the stream to have as many partitions
   * as the size of {@code partitions} map. Key of the map {@code partitions} represents the {@code partitionId} of
   * each {@link org.apache.samza.Partition} for a {@link org.apache.samza.system.SystemStreamPartition} and value is
   * an Iterable of messages that the {@link org.apache.samza.system.SystemStreamPartition} should be initialized with.
   *
   * @param systemName represents name of the system stream is associated with
   * @param streamName represents the name of the Stream
   * @param partitions Key of an entry in partitions represents a {@code partitionId} of a {@link org.apache.samza.Partition}
   *                   and value represents the stream of messages the {@link org.apache.samza.system.SystemStreamPartition}
   *                   will be initialized with
   * @param <T> represents the type of a message in the stream
   * @return a {@link CollectionStream} with multiple partitions each containing messages of the type {@code T}
   *
   */
  public static <T> CollectionStream<T> of(String systemName, String streamName, Map<Integer, ? extends Iterable<T>> partitions) {
    return new CollectionStream<>(systemName, streamName, partitions);
  }
}
