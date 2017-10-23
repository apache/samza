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

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.ApplicationRunner;

/**
 * This class defines the methods to create different types of Samza application instances: 1) high-level end-to-end
 * {@link StreamApplication}; 2) task-level StreamTaskApplication; 3) task-level AsyncStreamTaskApplication
 */
public class StreamApplications {
  //TODO: add the static map of all created application instances from the user program
  private static final Map<String, StreamApplication> USER_APPS = new HashMap<>();

  private StreamApplications() {

  }

  public static StreamApplication createStreamApp(Config config) {
    ApplicationRunner runner = ApplicationRunner.fromConfig(config);
    StreamApplication app = new StreamApplication(runner, config);
    USER_APPS.put(app.getGlobalAppId(), app);
    return app;
  }

}
