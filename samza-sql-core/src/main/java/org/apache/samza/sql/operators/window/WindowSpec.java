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

package org.apache.samza.sql.operators.window;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.operators.SimpleOperatorSpec;


/**
 * This class implements the specification class for the build-in {@link org.apache.samza.sql.operators.window.BoundedTimeWindow} operator
 */
public class WindowSpec extends SimpleOperatorSpec implements OperatorSpec {

  /**
   * The window size in seconds
   */
  private final int wndSizeSec;

  /**
   * Default ctor of the {@code WindowSpec} object
   *
   * @param id The identifier of the operator
   * @param input The input stream entity
   * @param output The output relation entity
   * @param lengthSec The window size in seconds
   */
  public WindowSpec(String id, EntityName input, EntityName output, int lengthSec) {
    super(id, input, output);
    this.wndSizeSec = lengthSec;
  }

  public WindowSpec(String id, int wndSize, String input) {
    super(id, EntityName.getStreamName(input), null);
    this.wndSizeSec = wndSize;
  }

  /**
   * Method to get the window state relation name
   *
   * @return The window state relation name
   */
  public String getWndStatesName() {
    return this.getId() + "-wnd-state";
  }

  /**
   * Method to get the window size in seconds
   *
   * @return The window size in seconds
   */
  public int getWndSizeSec() {
    return this.wndSizeSec;
  }
}
