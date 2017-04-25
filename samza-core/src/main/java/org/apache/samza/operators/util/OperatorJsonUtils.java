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

package org.apache.samza.operators.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.operators.spec.OperatorSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorJsonUtils {
  private static final Logger log = LoggerFactory.getLogger(OperatorJsonUtils.class);

  private static final String OP_CODE = "opCode";
  private static final String OP_ID = "opId";
  private static final String SOURCE_LOCATION = "sourceLocation";
  private static final String NEXT_OPERATOR_IDS = "nextOperatorIds";

  public static Map<String, Object> operatorToJson(OperatorSpec spec, Map<String, Object> properties) {
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put(OP_CODE, spec.getOpCode().name());
    jsonMap.put(OP_ID, spec.getOpId());
    jsonMap.put(SOURCE_LOCATION,
        String.format("%s:%s", spec.getSourceLocation().getFileName(), spec.getSourceLocation().getLineNumber()));

    if (spec.getNextStream() != null) {
      Collection<OperatorSpec> nextOperators = spec.getNextStream().getRegisteredOperatorSpecs();
      jsonMap.put(NEXT_OPERATOR_IDS, nextOperators.stream().map(OperatorSpec::getOpId).collect(Collectors.toSet()));
    } else {
      jsonMap.put(NEXT_OPERATOR_IDS, Collections.emptySet());
    }

    jsonMap.putAll(properties);
    return jsonMap;
  }
}
