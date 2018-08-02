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
package org.apache.samza.operators.descriptors.base.stream;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.operators.descriptors.base.system.ExpandingSystemDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.operators.descriptors.base.system.TransformingSystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemStreamMetadata.OffsetType;

/**
 * The base descriptor for an input stream. Allows setting properties that are common to all input streams.
 *
 * @param <StreamMessageType> type of messages in this stream.
 * @param <SubClass> type of the concrete sub-class
 */
@SuppressWarnings("unchecked")
public abstract class InputDescriptor<StreamMessageType, SubClass extends InputDescriptor<StreamMessageType, SubClass>>
    extends StreamDescriptor<StreamMessageType, SubClass> {
  private static final String RESET_OFFSET_CONFIG_KEY = "streams.%s.samza.reset.offset";
  private static final String OFFSET_DEFAULT_CONFIG_KEY = "streams.%s.samza.offset.default";
  private static final String PRIORITY_CONFIG_KEY = "streams.%s.samza.priority";
  private static final String BOOTSTRAP_CONFIG_KEY = "streams.%s.samza.bootstrap";
  private static final String BOUNDED_CONFIG_KEY = "streams.%s.samza.bounded";
  private static final String DELETE_COMMITTED_MESSAGES_CONFIG_KEY = "streams.%s.samza.delete.committed.messages";

  private Optional<InputTransformer<StreamMessageType>> transformerOptional = Optional.empty();
  private Optional<Boolean> resetOffsetOptional = Optional.empty();
  private Optional<OffsetType> offsetDefaultOptional = Optional.empty();
  private Optional<Integer> priorityOptional = Optional.empty();
  private Optional<Boolean> isBootstrapOptional = Optional.empty();
  private Optional<Boolean> isBoundedOptional = Optional.empty();
  private Optional<Boolean> shouldDeleteCommittedMessagesOptional = Optional.empty();

  /**
   * Constructs an {@link InputDescriptor} instance.
   *
   * @param streamId id of the stream
   * @param systemName system name for the stream
   * @param serde serde for messages in the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from if available, else null
   * @param transformer stream level input stream transform function if available, else null
   */
  public InputDescriptor(String streamId, String systemName, Serde serde,
      SystemDescriptor systemDescriptor, InputTransformer<StreamMessageType> transformer) {
    super(streamId, systemName, serde, systemDescriptor);

    // stream level transformer takes precedence over system level transformer
    if (transformer != null) {
      this.transformerOptional = Optional.of(transformer);
    } else {
      if (systemDescriptor instanceof TransformingSystemDescriptor) {
        this.transformerOptional =
            Optional.ofNullable(((TransformingSystemDescriptor) systemDescriptor).getTransformer());
      } else if (systemDescriptor instanceof ExpandingSystemDescriptor) {
        this.transformerOptional =
            Optional.ofNullable(((ExpandingSystemDescriptor) systemDescriptor).getTransformer());
      }
    }
  }

  /**
   * If set to true, when a Samza container starts up, it ignores any checkpointed offset for this particular
   * input stream. Its behavior is thus determined by the {@link #withOffsetDefault} setting.
   * Note that the reset takes effect every time a container is started, which may be every time you restart your job,
   * or more frequently if a container fails and is restarted by the framework.
   *
   * @param resetOffset whether the container should ignore any checkpointed offset when starting
   * @return this input descriptor
   */
  public SubClass withResetOffset(boolean resetOffset) {
    this.resetOffsetOptional = Optional.of(resetOffset);
    return (SubClass) this;
  }

  /**
   * If a container starts up without a checkpoint, this property determines where in the input stream we should start
   * consuming. The value must be an OffsetType, one of the following:
   * <ul>
   *  <li>upcoming: Start processing messages that are published after the job starts.
   *                Any messages published while the job was not running are not processed.
   *  <li>oldest: Start processing at the oldest available message in the system,
   *              and reprocess the entire available message history.
   * </ul>
   * This property is for an individual stream. To set it for all streams within a system, see
   * {@link SystemDescriptor#withDefaultStreamOffsetDefault}. If both are defined, the stream-level definition
   * takes precedence.
   *
   * @param offsetDefault offset type to start processing from
   * @return this input descriptor
   */
  public SubClass withOffsetDefault(OffsetType offsetDefault) {
    this.offsetDefaultOptional = Optional.ofNullable(offsetDefault);
    return (SubClass) this;
  }

  /**
   * If one or more streams have a priority set (any positive integer), they will be processed with higher priority
   * than the other streams.
   * <p>
   * You can set several streams to the same priority, or define multiple priority levels by assigning a
   * higher number to the higher-priority streams.
   * <p>
   * If a higher-priority stream has any messages available, they will always be processed first;
   * messages from lower-priority streams are only processed when there are no new messages on higher-priority inputs.
   *
   * @param priority priority for this input stream
   * @return this input descriptor
   */
  public SubClass withPriority(int priority) {
    this.priorityOptional = Optional.of(priority);
    return (SubClass) this;
  }

  /**
   * If set to true, this stream will be processed as a bootstrap stream. This means that every time a Samza container
   * starts up, this stream will be fully consumed before messages from any other stream are processed.
   *
   * @param bootstrap whether this stream should be processed as a bootstrap stream
   * @return this input descriptor
   */
  public SubClass withBootstrap(boolean bootstrap) {
    this.isBootstrapOptional = Optional.of(bootstrap);
    return (SubClass) this;
  }

  /**
   * If set to true, this stream will be considered a bounded stream. If all input streams in an application are
   * bounded, the job is considered to be running in batch processing mode.
   *
   * @param isBounded whether this stream is a bounded
   * @return this input descriptor
   */
  public SubClass withBounded(boolean isBounded) {
    this.isBoundedOptional = Optional.of(isBounded);
    return (SubClass) this;
  }

  /**
   * If set to true, and supported by the system implementation, messages older than the latest checkpointed offset
   * for this stream may be deleted after the commit.
   *
   * @param shouldDeleteCommittedMessages whether the system should attempt to delete checkpointed messages
   * @return this input descriptor
   */
  public SubClass withDeleteCommittedMessages(boolean shouldDeleteCommittedMessages) {
    this.shouldDeleteCommittedMessagesOptional = Optional.of(shouldDeleteCommittedMessages);
    return (SubClass) this;
  }

  public Optional<InputTransformer<StreamMessageType>> getTransformer() {
    return this.transformerOptional;
  }

  private Optional<Boolean> isResetOffset() {
    return this.resetOffsetOptional;
  }

  private Optional<OffsetType> getOffsetDefault() {
    return this.offsetDefaultOptional;
  }

  private Optional<Integer> getPriority() {
    return this.priorityOptional;
  }

  private Optional<Boolean> isBootstrap() {
    return this.isBootstrapOptional;
  }

  private Optional<Boolean> isBounded() {
    return this.isBoundedOptional;
  }

  private Optional<Boolean> shouldDeleteCommittedMessages() {
    return this.shouldDeleteCommittedMessagesOptional;
  }

  @Override
  public Map<String, String> toConfig() {
    HashMap<String, String> configs = new HashMap<>(super.toConfig());
    String streamId = getStreamId();
    getOffsetDefault().ifPresent(od -> configs.put(String.format(OFFSET_DEFAULT_CONFIG_KEY, streamId), od.name().toLowerCase()));
    isResetOffset().ifPresent(resetOffset ->
        configs.put(String.format(RESET_OFFSET_CONFIG_KEY, streamId), Boolean.toString(resetOffset)));
    getPriority().ifPresent(priority ->
        configs.put(String.format(PRIORITY_CONFIG_KEY, streamId), Integer.toString(priority)));
    isBootstrap().ifPresent(bootstrap ->
        configs.put(String.format(BOOTSTRAP_CONFIG_KEY, streamId), Boolean.toString(bootstrap)));
    isBounded().ifPresent(bounded ->
        configs.put(String.format(BOUNDED_CONFIG_KEY, streamId), Boolean.toString(bounded)));
    shouldDeleteCommittedMessages().ifPresent(deleteCommittedMessages ->
        configs.put(String.format(DELETE_COMMITTED_MESSAGES_CONFIG_KEY, streamId),
            Boolean.toString(deleteCommittedMessages)));
    return Collections.unmodifiableMap(configs);
  }
}
