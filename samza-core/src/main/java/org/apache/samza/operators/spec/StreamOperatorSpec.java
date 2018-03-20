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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.task.TaskContext;


/**
 * The spec for a simple stream operator that outputs 0 or more messages for each input message.
 *
 * @param <M>  the type of input message
 * @param <OM>  the type of output message
 */
public class StreamOperatorSpec<M, OM> extends OperatorSpec<M, OM> {

  private final FlatMapFunction<M, OM> transformFn;

  /**
   * Constructor for a {@link StreamOperatorSpec}.
   *
   * @param transformFn  the transformation function
   * @param opCode  the {@link OpCode} for this {@link StreamOperatorSpec}
   * @param opId  the unique ID for this {@link StreamOperatorSpec}
   */
  StreamOperatorSpec(FlatMapFunction<M, OM> transformFn, OperatorSpec.OpCode opCode, String opId) throws IOException {
    super(opCode, opId);
    this.transformFn = transformFn;
    // TODO: initWatermarkAndTimerFunctions()
  }

  StreamOperatorSpec(MapFunction<M, OM> mapFn, OperatorSpec.OpCode opCode, String opId) throws IOException {
    super(opCode, opId);
    this.transformFn = new FlatMapFunction<M, OM>() {
      @Override
      public Collection<OM> apply(M message) {
        return new ArrayList<OM>() {
          {
            OM r = mapFn.apply(message);
            if (r != null) {
              this.add(r);
            }
          }
        };
      }

      @Override
      public void init(Config config, TaskContext context) {
        mapFn.init(config, context);
      }

      @Override
      public void close() {
        mapFn.close();
      }
    };
  }

  public StreamOperatorSpec(FilterFunction<M> filterFn, OperatorSpec.OpCode opCode, String opId) throws IOException {
    super(opCode, opId);
    this.transformFn = new FlatMapFunction<M, OM>() {
      @Override
      public Collection<OM> apply(M message) {
        return new ArrayList<OM>() {
          {
            if (filterFn.apply(message)) {
              this.add((OM) message);
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
    };
  }

  public FlatMapFunction<M, OM> getTransformFn() {
    return this.transformFn;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return this.transformFn instanceof WatermarkFunction ? (WatermarkFunction) this.transformFn : null;
  }

  @Override
  public TimerFunction getTimerFn() {
    return this.transformFn instanceof TimerFunction ? (TimerFunction) this.transformFn : null;
  }

  public StreamOperatorSpec<M, OM> copy() throws IOException, ClassNotFoundException {
    return (StreamOperatorSpec<M, OM>) super.copy();
  }
  // TODO: need to have an overriding method readObject() to initialize the timer/watermark functions
}
