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

package org.apache.samza.sql.client.interfaces;

import java.util.ArrayList;
import java.util.List;


/**
 * defines the enumeration of Commands of a certain type
 */
public interface CommandType {

  /**
   * @return list of names of all commands in this enumeration
   */
  static List<String> getAllCommands() {
    return new ArrayList<>();
  }

  /**
   * returns the name of the command
   * @return String
   */
  String getCommandName();

  /**
   * returns the description of the command
   * @return String
   */
  String getDescription();

  /**
   * returns the description of the command
   * @return String
   */
  String getUsage();

  /**
   * returns the flag which specifies if the arguments of this command are optional or not
   * @return true if arguments of this command are optional
   */
  boolean argsAreOptional();
}