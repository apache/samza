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

import org.apache.samza.operators.StreamGraphBuilder;
import org.apache.samza.config.Config;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in a remote cluster
 */
public class RemoteApplicationRunner extends AbstractApplicationRunner {

  public RemoteApplicationRunner(Config config) {
    super(config);
  }

  @Override public void run(StreamGraphBuilder app, Config config) {
    // TODO: add description of ProcessContext that is going to create a sub-DAG of the {@code graph}
    // TODO: actually instantiate the tasks and run the job, i.e.
    // 1. create all input/output/intermediate topics
    // 2. create the single job configuration
    // 3. execute JobRunner to submit the single job for the whole graph
  }

}
