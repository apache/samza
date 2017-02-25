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

package org.apache.samza.validation;

import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.metrics.MetricsAccessor;
import org.apache.samza.metrics.MetricsValidationFailureException;
import org.apache.samza.metrics.MetricsValidator;


public class MockMetricsValidator implements MetricsValidator {

  @Override
  public void init(Config config) {
  }

  @Override
  public void validate(MetricsAccessor accessor) throws MetricsValidationFailureException {
    Map<String, Long> commitCalls = accessor.getCounterValues(SamzaContainerMetrics.class.getName(), "commit-calls");
    if(commitCalls.isEmpty()) throw new MetricsValidationFailureException("no value");
    for(Map.Entry<String, Long> entry: commitCalls.entrySet()) {
      if(entry.getValue() <= 0) {
        throw new MetricsValidationFailureException("commit call <= 0");
      }
    }
  }

  @Override
  public void complete() {
  }
}