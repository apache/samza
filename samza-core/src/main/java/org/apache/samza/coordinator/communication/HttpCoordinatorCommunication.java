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
package org.apache.samza.coordinator.communication;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.samza.coordinator.server.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Runs an {@link HttpServer} to communicate with workers.
 */
public class HttpCoordinatorCommunication implements CoordinatorCommunication {
  private static final Logger LOG = LoggerFactory.getLogger(HttpCoordinatorCommunication.class);

  private final HttpServer httpServer;

  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  public HttpCoordinatorCommunication(HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  @Override
  public void start() {
    if (!this.isRunning.compareAndSet(false, true)) {
      LOG.warn("Tried to start, but already started");
    } else {
      this.httpServer.start();
      LOG.info("Started http server");
    }
  }

  @Override
  public void stop() {
    if (!this.isRunning.compareAndSet(true, false)) {
      LOG.warn("Tried to stop, but not currently running");
    } else {
      this.httpServer.stop();
      LOG.info("Stopped http server");
    }
  }
}
