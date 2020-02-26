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
package org.apache.samza.lineage;

/**
 * The LineageReporter interface defines how Samza writes job lineage information to outside systems, such as messaging
 * systems like Kafka, or file systems.
 * Implementations are responsible for accepting lineage data and writing them to their backend systems.
 */
public interface LineageReporter<T> {

  /**
   * Start the reporter.
   */
  void start();

  /**
   * Stop the reporter.
   */
  void stop();

  /**
   * Send the specified Samza job lineage data to outside system.
   * @param lineage Samza job lineage data model
   */
  void report(T lineage);
}
