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
package org.apache.samza.job.yarn.util;

import org.apache.samza.coordinator.server.HttpServer;
import org.eclipse.jetty.servlet.ServletHolder;

import java.net.MalformedURLException;
import java.net.URL;

public class MockHttpServer extends HttpServer {

  public MockHttpServer(String rootPath, int port, String resourceBasePath, ServletHolder defaultHolder) {
    super(rootPath, port, resourceBasePath, defaultHolder);
    start();
  }

  @Override
  public void start() {
    super.running_$eq(true);
  }

  @Override
  public void stop() {
    super.running_$eq(false);
  }

  @Override
  public URL getUrl() {
    if(running()) {
      try {
        return new URL("http://localhost:12345/");
      } catch (MalformedURLException mue) {
        mue.printStackTrace();
      }
    }
    return null;
  }
}
