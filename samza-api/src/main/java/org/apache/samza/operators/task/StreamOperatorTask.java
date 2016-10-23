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
package org.apache.samza.operators.task;

import org.apache.samza.operators.MessageStreams.SystemMessageStream;
import org.apache.samza.operators.data.IncomingSystemMessage;

import java.util.Collection;

/**
 * This interface defines the methods that user needs to implement via the operator programming APIs.
 */
public interface StreamOperatorTask {

  /**
   * Defines the method for users to initialize the operator chains consuming from all {@link SystemMessageStream}s.
   * Users have to implement this function to instantiate {@link org.apache.samza.operators.internal.OperatorChain} that
   * will process each incoming {@link SystemMessageStream}.
   *
   * Note that each {@link SystemMessageStream} corresponds to an input {@link org.apache.samza.system.SystemStreamPartition}
   *
   * @param sources  the collection of {@link SystemMessageStream}s that takes {@link IncomingSystemMessage}
   *                 from a {@link org.apache.samza.system.SystemStreamPartition}
   */
  void initOperators(Collection<SystemMessageStream> sources);


}
