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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.data.MessageEnvelope;


/**
 * A function that joins {@link MessageEnvelope}s from two {@link org.apache.samza.operators.MessageStream}s and produces
 * a joined {@link MessageEnvelope}.
 * @param <M>  type of the input {@link MessageEnvelope}
 * @param <JM>  type of the {@link MessageEnvelope} to join with
 * @param <RM>  type of the joined {@link MessageEnvelope}
 */
@InterfaceStability.Unstable
@FunctionalInterface
public interface JoinFunction<M extends MessageEnvelope, JM extends MessageEnvelope, RM extends MessageEnvelope> {

  /**
   * Join the provided {@link MessageEnvelope}s and produces the joined {@link MessageEnvelope}.
   * @param message  the input {@link MessageEnvelope}
   * @param otherMessage  the {@link MessageEnvelope} to join with
   * @return  the joined {@link MessageEnvelope}
   */
  RM apply(M message, JM otherMessage);

}
