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
package org.apache.samza.operators;

import org.apache.samza.operators.data.IncomingSystemMessage;
import org.apache.samza.system.SystemStreamPartition;


/**
 * This class defines all methods to create a {@link MessageStream} object. Users can use this to create an {@link MessageStream}
 * from a specific input source.
 *
 */

public final class MessageStreams {

  /**
   * private constructor to prevent instantiation
   */
  private MessageStreams() {}

  /**
   * private class for system input/output {@link MessageStream}
   */
  public static final class SystemMessageStream extends MessageStream<IncomingSystemMessage> {
    /**
     * The corresponding {@link org.apache.samza.system.SystemStream}
     */
    private final SystemStreamPartition ssp;

    /**
     * Constructor for input system stream
     *
     * @param ssp  the input {@link SystemStreamPartition} for the input {@link SystemMessageStream}
     */
    private SystemMessageStream(SystemStreamPartition ssp) {
      this.ssp = ssp;
    }

    /**
     * Getter for the {@link SystemStreamPartition} of the input
     *
     * @return the input {@link SystemStreamPartition}
     */
    public SystemStreamPartition getSystemStreamPartition() {
      return this.ssp;
    }
  }

  /**
   * Public static API methods start here
   */

  /**
   * Static API method to create a {@link MessageStream} from a system input stream
   *
   * @param ssp  the input {@link SystemStreamPartition}
   * @return the {@link MessageStream} object takes {@code ssp} as the input
   */
  public static SystemMessageStream input(SystemStreamPartition ssp) {
    return new SystemMessageStream(ssp);
  }

}
