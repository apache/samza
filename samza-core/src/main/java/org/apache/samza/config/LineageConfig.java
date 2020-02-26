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
package org.apache.samza.config;

import java.util.Optional;


/**
 * Config helper methods related to Samza job lineage
 */
public class LineageConfig extends MapConfig {

  public static final String LINEAGE_FACTORY = "lineage.factory";
  public static final String LINEAGE_REPORTER_FACTORY = "lineage.reporter.factory";
  public static final String LINEAGE_REPORTER_STREAM = "lineage.reporter.stream";

  public LineageConfig(Config config) {
    super(config);
  }

  public Optional<String> getLineageFactoryClassName() {
    return Optional.ofNullable(get(LINEAGE_FACTORY));
  }

  public Optional<String> getLineageReporterFactoryClassName() {
    return Optional.ofNullable(get(LINEAGE_REPORTER_FACTORY));
  }

  public Optional<String> getLineageReporterStreamName() {
    return Optional.ofNullable(get(LINEAGE_REPORTER_STREAM));
  }
}
