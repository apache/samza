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
import org.apache.samza.operators.data.Message;


/**
 * A function that specifies whether a {@link Message} should be retained for further processing or filtered out.
 * @param <M>  type of the input {@link Message}
 */
@InterfaceStability.Unstable
@FunctionalInterface
public interface FilterFunction<M extends Message> {

  /**
   * Returns a boolean indicating whether this {@link Message} should be retained or filtered out.
   * @param message  the message to be checked
   * @return  true if message should be retained, else false
   */
  boolean apply(M message);

}
