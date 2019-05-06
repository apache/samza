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
package org.apache.samza.startpoint;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamPartition;

/**
 * A {@link SystemAdmin} implementation should implement this abstraction to support {@link Startpoint}.
 */
@InterfaceStability.Evolving
public interface StartpointVisitor {

  /**
   * Resolves the {@link StartpointSpecific} to a system specific offset.
   * @param systemStreamPartition the {@link SystemStreamPartition} of the startpoint.
   * @param startpointSpecific the {@link Startpoint} that represents the specific offset.
   * @return the resolved offset.
   */
  default String visit(SystemStreamPartition systemStreamPartition, StartpointSpecific startpointSpecific) {
    return startpointSpecific.getSpecificOffset();
  }

  /**
   * Resolves the timestamp in {@link StartpointTimestamp} to a system specific offset.
   * @param systemStreamPartition the {@link SystemStreamPartition} of the startpoint.
   * @param startpointTimestamp the {@link Startpoint} that represents the timestamp.
   * @return the resolved offset.
   */
  default String visit(SystemStreamPartition systemStreamPartition, StartpointTimestamp startpointTimestamp) {
    throw new UnsupportedOperationException("StartpointTimestamp is not supported.");
  }

  /**
   * Resolves to the earliest offset of the {@link SystemStreamPartition}.
   * @param systemStreamPartition the {@link SystemStreamPartition} of the startpoint.
   * @param startpointOldest the {@link Startpoint} that represents the earliest offset.
   * @return the resolved offset.
   */
  default String visit(SystemStreamPartition systemStreamPartition, StartpointOldest startpointOldest) {
    throw new UnsupportedOperationException("StartpointOldest is not supported.");
  }

  /**
   * Resolves to the latest offset of {@link SystemStreamPartition}.
   * @param systemStreamPartition the {@link SystemStreamPartition} of the startpoint.
   * @param startpointUpcoming the {@link Startpoint} that represents the latest offset.
   * @return the resolved offset.
   */
  default String visit(SystemStreamPartition systemStreamPartition, StartpointUpcoming startpointUpcoming) {
    throw new UnsupportedOperationException("StartpointUpcoming is not supported.");
  }
}
