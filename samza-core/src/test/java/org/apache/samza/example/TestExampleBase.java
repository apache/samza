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
package org.apache.samza.example;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Base class for test examples
 *
 */
public abstract class TestExampleBase implements StreamApplication {

  protected final Map<SystemStream, Set<SystemStreamPartition>> inputs;

  TestExampleBase(Set<SystemStreamPartition> inputs) {
    this.inputs = new HashMap<>();
    for (SystemStreamPartition input : inputs) {
      this.inputs.putIfAbsent(input.getSystemStream(), new HashSet<>());
      this.inputs.get(input.getSystemStream()).add(input);
    }
  }

}
