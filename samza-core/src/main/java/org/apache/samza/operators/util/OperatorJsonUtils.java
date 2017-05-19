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
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.stream.OutputStreamInternal;

public class OperatorJsonUtils {
  private static final String OP_CODE = "opCode";
  private static final String OP_ID = "opId";
  private static final String SOURCE_LOCATION = "sourceLocation";
  private static final String NEXT_OPERATOR_IDS = "nextOperatorIds";
  private static final String OUTPUT_STREAM_ID = "outputStreamId";
  private static final String TTL_MS = "ttlMs";

  /**
   * Format the operator properties into a map
   * @param spec a {@link OperatorSpec} instance
   * @return map of the operator properties
   */
  public static Map<String, Object> operatorToMap(OperatorSpec spec) {
    Map<String, Object> map = new HashMap<>();
    map.put(OP_CODE, spec.getOpCode().name());
    map.put(OP_ID, spec.getOpId());
    map.put(SOURCE_LOCATION, spec.getSourceLocation());

    if (spec.getNextStream() != null) {
      Collection<OperatorSpec> nextOperators = spec.getNextStream().getRegisteredOperatorSpecs();
      map.put(NEXT_OPERATOR_IDS, nextOperators.stream().map(OperatorSpec::getOpId).collect(Collectors.toSet()));
    } else {
      map.put(NEXT_OPERATOR_IDS, Collections.emptySet());
    }

    if (spec instanceof SinkOperatorSpec) {
      OutputStreamInternal outputStream = ((SinkOperatorSpec) spec).getOutputStream();
      if (outputStream != null) {
        map.put(OUTPUT_STREAM_ID, outputStream.getStreamSpec().getId());
      }
    }

    if (spec instanceof PartialJoinOperatorSpec) {
      map.put(TTL_MS, ((PartialJoinOperatorSpec) spec).getTtlMs());
    }

    return map;
  }

  /**
   * Return the location of source code that creates the operator.
   * This function is invoked in the constructor of each operator.
   * @return formatted source location including file and line number
   */
  public static String getSourceLocation() {
    // The stack trace looks like:
    // [0] Thread.getStackTrace()
    // [1] OperatorJsonUtils.getSourceLocation()
    // [2] SomeOperator.<init>()
    // [3] OperatorSpecs.createSomeOperator()
    // [4] MessageStreamImpl.someOperator()
    // [5] User code that calls [2]
    // we are only interested in [5] here
    StackTraceElement location = Thread.currentThread().getStackTrace()[5];
    return String.format("%s:%s", location.getFileName(), location.getLineNumber());
  }
}
