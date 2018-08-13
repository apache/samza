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
package org.apache.samza.application;

/**
 * The base interface for all user-implemented applications in Samza. The main processing logic of the user application
 * should be implemented in {@link ApplicationBase#describe(ApplicationDescriptor)} method. Sub-classes {@link StreamApplication}
 * and {@link TaskApplication} are specific interfaces for applications written in high-level DAG and low-level task APIs,
 * respectively.
 */
public interface ApplicationBase<S extends ApplicationDescriptor> {

  /**
   * Describes the user processing logic via {@link ApplicationDescriptor}
   *
   * @param appDesc the {@link ApplicationDescriptor} object to describe user application logic
   */
  void describe(S appDesc);
}
