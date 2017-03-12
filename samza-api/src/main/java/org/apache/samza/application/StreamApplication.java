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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraph;


/**
 * This interface defines a factory class that user will implement to create user-defined operator DAG in a {@link StreamGraph} object.
 */
@InterfaceStability.Unstable
public interface StreamApplication {
  static final String APP_CLASS_CONFIG = "app.class";

  /**
   * Users are required to implement this abstract method to initialize the processing logic of the application, in terms
   * of a DAG of {@link org.apache.samza.operators.MessageStream}s and operators
   *
   * @param graph  an empty {@link StreamGraph} object to be initialized
   * @param config  the {@link Config} of the application
   */
  void init(StreamGraph graph, Config config);
}
