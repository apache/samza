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
public interface StartpointConsumerVisitor {

  /**
   * Seek to specific offset represented by {@link StartpointSpecific}
   * @param systemStreamPartition The {@link SystemStreamPartition} to seek the offset to.
   * @param startpointSpecific The {@link Startpoint} that represents the specific offset.
   */
  void register(SystemStreamPartition systemStreamPartition, StartpointSpecific startpointSpecific);

  /**
   * Seek to timestamp offset represented by {@link StartpointTimestamp}
   * @param systemStreamPartition The {@link SystemStreamPartition} to seek the offset to.
   * @param startpointTimestamp The {@link Startpoint} that represents the timestamp offset.
   */
  void register(SystemStreamPartition systemStreamPartition, StartpointTimestamp startpointTimestamp);

  /**
   * Seek to earliest offset represented by {@link StartpointEarliest}
   * @param systemStreamPartition The {@link SystemStreamPartition} to seek the offset to.
   * @param startpointEarliest The {@link Startpoint} that represents the earliest offset.
   */
  void register(SystemStreamPartition systemStreamPartition, StartpointEarliest startpointEarliest);

  /**
   * Seek to latest offset represented by {@link StartpointLatest}
   * @param systemStreamPartition The {@link SystemStreamPartition} to seek the offset to.
   * @param startpointLatest The {@link Startpoint} that represents the latest offset.
   */
  void register(SystemStreamPartition systemStreamPartition, StartpointLatest startpointLatest);

  /**
   * Bootstrap signal represented by {@link StartpointBootstrap}
   * @param systemStreamPartition The {@link SystemStreamPartition} to seek the offset to.
   * @param startpointBootstrap The {@link Startpoint} that represents the bootstrap signal.
   */
  void register(SystemStreamPartition systemStreamPartition, StartpointBootstrap startpointBootstrap);
}
