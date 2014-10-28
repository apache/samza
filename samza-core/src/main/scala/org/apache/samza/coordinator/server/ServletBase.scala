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

package org.apache.samza.coordinator.server;

import java.io.IOException
import javax.servlet.ServletException
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.samza.serializers.model.SamzaObjectMapper

/**
 * A simple servlet helper that makes it easy to dump objects to JSON.
 */
trait ServletBase extends HttpServlet {
  val mapper = SamzaObjectMapper.getObjectMapper()

  override protected def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("application/json")
    response.setStatus(HttpServletResponse.SC_OK)
    mapper.writeValue(response.getWriter(), getObjectToWrite())
  }

  /**
   * Returns an object that should be fed to Jackson's ObjectMapper, and 
   * returned as an HTTP response.
   */
  protected def getObjectToWrite(): Object
}
