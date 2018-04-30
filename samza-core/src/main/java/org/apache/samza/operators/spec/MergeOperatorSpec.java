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
package org.apache.samza.operators.spec;

import java.util.ArrayList;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;


/**
 * The spec for an operator that combines messages from all input streams into a single output stream. This is a package-private
 * class since the constructor of the class is only accessed via {@link OperatorSpecs} and the runtime reference to the object
 * is always upcast to {@link StreamOperatorSpec},
 *
 * @param <M> the type of messages in all input streams
 */
class MergeOperatorSpec<M> extends StreamOperatorSpec<M, M> {

  MergeOperatorSpec(String opId) {
    super((M message) ->
        new ArrayList<M>() {
        {
          this.add(message);
        }
      }, OperatorSpec.OpCode.MERGE, opId);
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return null;
  }

  @Override
  public TimerFunction getTimerFn() {
    return null;
  }
}
