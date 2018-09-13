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
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.serializers.NoOpSerde;

/**
 * A descriptor for a In Memory output stream.
 * <p>
 * An instance of this descriptor may be obtained from an appropriately configured {@link InMemorySystemDescriptor}.
 * <p>
 * Stream properties configured using a descriptor override corresponding properties provided in configuration.
 *
 * @param <StreamMessageType> type of messages in this stream.
 */
public class InMemoryOutputDescriptor<StreamMessageType>
    extends OutputDescriptor<StreamMessageType, InMemoryOutputDescriptor<StreamMessageType>> {

  private int partitionCount;

  /**
   * Constructs an {@link OutputDescriptor} instance.
   * @param streamId id of the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from
   */
  InMemoryOutputDescriptor(String streamId, SystemDescriptor systemDescriptor) {
    super(streamId, new NoOpSerde<>(), systemDescriptor);
  }

  /**
   * Configures an InMemory output stream with InMemory system
   * @param partitionCount partition count of output stream
   * @return this output descriptor
   */
  public InMemoryOutputDescriptor<StreamMessageType> withPartitionCount(int partitionCount) {
    Preconditions.checkState(partitionCount > 0);
    this.partitionCount = partitionCount;
    return this;
  }

  public Integer getPartitionCount() {
    return this.partitionCount;
  }
}
