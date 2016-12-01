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

import java.util.Collection;


/**
 * A function that transforms a {@link MessageEnvelope} into a collection of 0 or more {@link MessageEnvelope}s,
 * possibly of a different type.
 * @param <M>  type of the input {@link MessageEnvelope}
 * @param <OM>  type of the transformed {@link MessageEnvelope}s
 */
@InterfaceStability.Unstable
@FunctionalInterface
public interface FlatMapFunction<M extends MessageEnvelope, OM extends MessageEnvelope> {

  /**
   * Transforms the provided {@link MessageEnvelope} into a collection of 0 or more {@link MessageEnvelope}s.
   * @param message  the {@link MessageEnvelope} to be transformed
   * @return  a collection of 0 or more transformed {@link MessageEnvelope}s
   */
  Collection<OM> apply(M message);

}
