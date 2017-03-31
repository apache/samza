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
package org.apache.samza.runtime;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;

@InterfaceStability.Evolving
public interface ProcessorIdGenerator {
  /**
   * Generates a String representation to identify a single instance of StreamProcessor
   * This value can be representative of its current executing environment. It can also be custom-managed by the user.
   *
   * <b>Note</b>: processorId has to be unique among the processors within a job. Processors can co-exists within the
   * same JVM, as long as the identifiers are guaranteed to be unique.
   *
   * @param config Config instance
   * @return String Identifier for the processor
   */
  String generateProcessorId(Config config);
}
