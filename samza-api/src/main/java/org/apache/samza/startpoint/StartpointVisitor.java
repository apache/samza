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

import org.apache.samza.system.SystemStreamPartition;


/**
 * Visitor interface for system consumers to implement to support {@link Startpoint}s.
 */
public interface StartpointVisitor {

  /**
   * Seek to specific offset represented by {@link StartpointSpecific}
   * @param systemStreamPartition The {@link SystemStreamPartition} to seek the offset to.
   * @param startpointSpecific The {@link Startpoint} that represents the specific offset.
   */
  void visit(SystemStreamPartition systemStreamPartition, StartpointSpecific startpointSpecific);

  /**
   * Seek to timestamp offset represented by {@link StartpointTimestamp}
   * @param systemStreamPartition The {@link SystemStreamPartition} to seek the offset to.
   * @param startpointTimestamp The {@link Startpoint} that represents the timestamp offset.
   */
  default void visit(SystemStreamPartition systemStreamPartition, StartpointTimestamp startpointTimestamp) {
    throw new UnsupportedOperationException("StartpointTimestamp is not supported.");
  }

  /**
   * Seek to earliest offset represented by {@link StartpointOldest}
   * @param systemStreamPartition The {@link SystemStreamPartition} to seek the offset to.
   * @param startpointOldest The {@link Startpoint} that represents the earliest offset.
   */
  default void visit(SystemStreamPartition systemStreamPartition, StartpointOldest startpointOldest) {
    throw new UnsupportedOperationException("StartpointOldest is not supported.");
  }

  /**
   * Seek to latest offset represented by {@link StartpointUpcoming}
   * @param systemStreamPartition The {@link SystemStreamPartition} to seek the offset to.
   * @param startpointUpcoming The {@link Startpoint} that represents the latest offset.
   */
  default void visit(SystemStreamPartition systemStreamPartition, StartpointUpcoming startpointUpcoming) {
    throw new UnsupportedOperationException("StartpointUpcoming is not supported.");
  }

  /**
   * Bootstrap signal represented by {@link StartpointCustom}
   * @param systemStreamPartition The {@link SystemStreamPartition} to seek the offset to.
   * @param startpointCustom The {@link Startpoint} that represents the bootstrap signal.
   */
  default void visit(SystemStreamPartition systemStreamPartition, StartpointCustom startpointCustom) {
    throw new UnsupportedOperationException(String.format("%s is not supported.", startpointCustom.getClass().getSimpleName()));
  }
}
