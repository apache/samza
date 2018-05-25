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
import java.util.Collection;
import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.task.TaskContext;


/**
 * The spec for an operator that filters input messages based on some conditions.
 *
 * @param <M> type of input message
 */
class FilterOperatorSpec<M> extends StreamOperatorSpec<M, M> {
  private final FilterFunction<M> filterFn;

  FilterOperatorSpec(FilterFunction<M> filterFn, String opId) {
    super(new FlatMapFunction<M, M>() {
      @Override
      public Collection<M> apply(M message) {
        return new ArrayList<M>() {
          {
            if (filterFn.apply(message)) {
              this.add(message);
            }
          }
        };
      }

      @Override
      public void init(Config config, TaskContext context) {
        filterFn.init(config, context);
      }

      @Override
      public void close() {
        filterFn.close();
      }
    }, OpCode.FILTER, opId);
    this.filterFn = filterFn;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return this.filterFn instanceof WatermarkFunction ? (WatermarkFunction) this.filterFn : null;
  }

  @Override
  public TimerFunction getTimerFn() {
    return this.filterFn instanceof TimerFunction ? (TimerFunction) this.filterFn : null;
  }
}
