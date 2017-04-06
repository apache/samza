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
   * Generates a String representation to identify a single instance of StreamProcessor.
   *
   * This value can be representative of its current executing environment. It can also be custom-managed by the user,
   * as long as it adheres to the specification below. More than one processor can co-exist within the same JVM,
   * as long as their identifiers are guaranteed to be unique.
   *
   * <b>Specification of processor identifier</b>:
   * <ul>
   *  <li>Processor identifier has to be unique among the processors within a job</li>
   *  <li>When more than one processor co-exist within the same JVM, the processor identifier should be of the format:
   *  $x_$y, where 'x' is a unique identifier for the executing JVM and 'y' is a unique identifier for the
   *  processor instance within the JVM.</li>
   * </ul>
   *
   * <b>Note</b>:
   * In case of more than one processors within the same JVM, the custom implementation of ProcessorIdGenerator can
   * contain a static counter, which is incremented on each call to generateProcessorId. The counter value can
   * be treated as the identifier for the processor instance within the JVM.
   *
   * @param config Config instance
   * @return String Identifier for the processor
   */
  String generateProcessorId(Config config);
}
