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

import java.util.Collections;
import javax.ws.rs.core.Response;

/**
 * This is a helper class that holds the methods that are reusable
 * across the different samza-rest resource endpoints.
 */
public class Responses {

  private Responses() {
  }

  /**
   * Constructs a consistent format for error responses.
   *
   * @param message the error message to report.
   * @return        the {@link Response} containing the error message.
   */
  public static Response errorResponse(String message) {
    return Response.serverError().entity(Collections.singletonMap("message", message)).build();
  }

  /**
   * Constructs a bad request (HTTP 400) response.
   *
   * @param message the bad request message to report.
   * @return        the {@link Response} containing the message.
   */
  public static Response badRequestResponse(String message) {
    return Response.status(Response.Status.BAD_REQUEST).entity(Collections.singletonMap("message", message)).build();
  }
}
