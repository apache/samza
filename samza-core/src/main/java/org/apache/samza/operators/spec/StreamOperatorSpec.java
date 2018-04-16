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

import java.io.Serializable;
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
  private final Serializable userFn;

  /**
   * Constructor for a {@link StreamOperatorSpec}.
   *
   * @param transformFn  the transformation function
   * @param opCode  the {@link OpCode} for this {@link StreamOperatorSpec}
   * @param opId  the unique ID for this {@link StreamOperatorSpec}
   */
  private StreamOperatorSpec(FlatMapFunction<M, OM> transformFn, OperatorSpec.OpCode opCode, String opId) {
    super(opCode, opId);
    this.transformFn = transformFn;
    this.userFn = transformFn;
  }

  private StreamOperatorSpec(MapFunction<M, OM> mapFn, OperatorSpec.OpCode opCode, String opId) {
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
    this.userFn = mapFn;
  }

  private StreamOperatorSpec(FilterFunction<M> filterFn, OperatorSpec.OpCode opCode, String opId) {
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
    this.userFn = filterFn;
  }

  public FlatMapFunction<M, OM> getTransformFn() {
    return this.transformFn;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return this.userFn instanceof WatermarkFunction ? (WatermarkFunction) this.userFn : null;
  }

  @Override
  public TimerFunction getTimerFn() {
    return this.userFn instanceof TimerFunction ? (TimerFunction) this.userFn : null;
  }

  public static <M, OM> StreamOperatorSpec<M, OM> createStreamOperatorSpec(MapFunction<M, OM> mapFn,
      OperatorSpec.OpCode opCode, String opId) {
    return new StreamOperatorSpec<M, OM>(mapFn, opCode, opId);
  }

  public static <M, OM> StreamOperatorSpec<M, OM> createStreamOperatorSpec(FlatMapFunction<M, OM> flatMapFn,
      OperatorSpec.OpCode opCode, String opId) {
    return new StreamOperatorSpec<M, OM>(flatMapFn, opCode, opId);
  }

  public static <M> StreamOperatorSpec<M, M> createStreamOperatorSpec(FilterFunction<M> filterFn,
      OperatorSpec.OpCode opCode, String opId) {
    return new StreamOperatorSpec<M, M>(filterFn, opCode, opId);
  }
}
