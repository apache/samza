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
package org.apache.samza.operators.api.data;

import org.apache.samza.system.SystemStreamPartition;


/**
 * This interface defines additional methods a message from an system input should implement, including the methods to
 * get {@link SystemStreamPartition} and the {@link Offset} of the input system message.
 */
public interface InputSystemMessage<O extends Offset> {

  /**
   * Get the input message's {@link SystemStreamPartition}
   *
   * @return  the {@link SystemStreamPartition} this message is coming from
   */
  SystemStreamPartition getSystemStreamPartition();

  /**
   * Get the offset of the message in the input stream. This should be used to uniquely identify a message in an input stream.
   *
   * @return The offset of the message in the input stream.
   */
  O getOffset();
}
