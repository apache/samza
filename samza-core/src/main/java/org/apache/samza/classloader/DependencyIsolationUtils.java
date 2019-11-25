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
package org.apache.samza.classloader;

public class DependencyIsolationUtils {
  /**
   * Directory inside the home directory of the cluster-based job coordinator in which the framework API artifacts are
   * placed, for usage in dependency isolation for the cluster-based job coordinator.
   * TODO make this configurable or taken from an environment variable
   */
  public static final String FRAMEWORK_API_DIRECTORY = "__samzaFrameworkApi";

  /**
   * Directory inside the home directory of the cluster-based job coordinator in which the framework infrastructure
   * artifacts are placed, for usage in dependency isolation for the cluster-based job coordinator.
   * TODO make this configurable or taken from an environment variable
   */
  public static final String FRAMEWORK_INFRASTRUCTURE_DIRECTORY = "__samzaFrameworkInfrastructure";

  /**
   * Directory inside the home directory of the cluster-based job coordinator in which the application artifacts are
   * placed, for usage in dependency isolation for the cluster-based job coordinator.
   * TODO make this configurable or taken from an environment variable
   */
  public static final String APPLICATION_DIRECTORY = "__package";

  /**
   * Name of the file which contains the class names (or globs) which should be loaded from the framework API
   * classloader.
   */
  public static final String FRAMEWORK_API_CLASS_LIST_FILE_NAME = "samza-framework-api-classes.txt";
}
