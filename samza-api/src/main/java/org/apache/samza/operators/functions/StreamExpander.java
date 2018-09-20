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
package org.apache.samza.operators.functions;

import java.io.Serializable;
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;

/**
 * Expands the provided {@link InputDescriptor} to a sub-DAG of one or more operators on the {@link StreamApplicationDescriptor},
 * and returns a new {@link MessageStream} with the combined results. Called when {@link StreamApplicationDescriptor#getInputStream}
 * is being used to get a {@link MessageStream} using an {@link InputDescriptor} from an expanding system descriptor.
 * <p>
 * This is provided by default by expanding system descriptor implementations and can not be overridden
 * or set on a per stream level.
 *
 * @param <OM> type of the messages in the resultant {@link MessageStream}
 */
public interface StreamExpander<OM> extends Serializable {

  /**
   * Expands the provided {@link InputDescriptor} to a sub-DAG of one or more operators on the {@link StreamApplicationDescriptor},
   * and returns a new {@link MessageStream} with the combined results. Called when the {@link InputDescriptor}
   * is being used to get an {@link MessageStream} using {@link StreamApplicationDescriptor#getInputStream}.
   * <p>
   * Notes for system implementers:
   * <p>
   * Take care to avoid infinite recursion in the implementation; e.g., by ensuring that it doesn't call
   * {@link StreamApplicationDescriptor#getInputStream} with an {@link InputDescriptor} from an expanding system descriptor
   * (like this one) again.
   * <p>
   * It's the {@link StreamExpander}'s responsibility to propagate any properties, including serde, from the
   * user-provided {@link InputDescriptor} to the expanded input descriptors.
   *
   * @param streamAppDesc the {@link StreamApplicationDescriptor} to register the expanded sub-DAG of operators on
   * @param inputDescriptor the {@link InputDescriptor} to be expanded
   * @return the {@link MessageStream} containing the combined results of the sub-DAG of operators
   */
  MessageStream<OM> apply(StreamApplicationDescriptor streamAppDesc, InputDescriptor inputDescriptor);

}
