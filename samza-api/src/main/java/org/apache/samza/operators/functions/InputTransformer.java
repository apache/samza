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
import org.apache.samza.system.IncomingMessageEnvelope;

/**
 * Transforms an {@link IncomingMessageEnvelope} with deserialized key and message to a message of type {@code OM}
 * which is delivered to the {@code MessageStream}. Called in {@code InputOperatorImpl} when incoming messages
 * from a {@code SystemConsumer} are being delivered to the application.
 * <p>
 * This is provided by default by transforming system descriptor implementations and can not be overridden
 * or set on a per stream level.
 *
 * @param <OM> type of the transformed message
 */
public interface InputTransformer<OM> extends InitableFunction, ClosableFunction, Serializable {

  /**
   * Transforms the provided {@link IncomingMessageEnvelope} with deserialized key and message into another message
   * which is delivered to the {@code MessageStream}.
   *
   * @param ime  the {@link IncomingMessageEnvelope} to be transformed
   * @return  the transformed message
   */
  OM apply(IncomingMessageEnvelope ime);

}
