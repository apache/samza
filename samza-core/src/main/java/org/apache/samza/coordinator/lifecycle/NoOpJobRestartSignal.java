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
package org.apache.samza.coordinator.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Placeholder implementation for {@link JobRestartSignal}.
 * If a use case requires job restarts, then a real implementation should be used.
 */
public class NoOpJobRestartSignal implements JobRestartSignal {
  private static final Logger LOG = LoggerFactory.getLogger(NoOpJobRestartSignal.class);

  @Override
  public void restartJob() {
    LOG.info("Job restart signalled, but job restart is no-op for this class");
  }
}
