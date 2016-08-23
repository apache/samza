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
package org.apache.samza.rest.resources;

import java.util.List;
import org.apache.samza.config.Config;


/**
 * Instantiates a resource using the provided config.
 *
 * This is used to instantiate and register a specific instance of the object rather than registering the class.
 */
public interface ResourceFactory {

  /**
   * Constructs and returns resource instances to register with the server.
   *
   * @param config  the server config used to initialize the objects.
   * @return        a collection of instances to register with the server.
   */
  List<? extends Object> getResourceInstances(Config config);
}
