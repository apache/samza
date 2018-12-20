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
 * A {@link Startpoint} that represents a custom startpoint. This is for systems that have a non-generic option
 * for setting offsets. Startpoints are serialized to JSON in the {@link org.apache.samza.metadatastore.MetadataStore}
 * and it is recommended to maintain the subclass of this {@link StartpointCustom} as a simple POJO.
 */
public abstract class StartpointCustom extends Startpoint {

  StartpointCustom() {
    super();
  }

  StartpointCustom(long creationTimestamp) {
    super(creationTimestamp);
  }

  @Override
  public void apply(SystemStreamPartition systemStreamPartition, StartpointVisitor startpointVisitor) {
    startpointVisitor.visit(systemStreamPartition, this);
  }
}
