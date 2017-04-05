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
package org.apache.samza.operators;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.system.StreamSpec;


/**
 * The interface class defining the specific {@link SinkFunction} for a system {@link OutputStream}.
 *
 * @param <M>  The type of message to be send to this output stream
 */
@InterfaceStability.Unstable
public interface OutputStream<M> {

  /**
   * Returns the specific {@link SinkFunction} for this output stream. The {@link OutputStream} is created
   * via {@link StreamGraph#createOutStream(StreamSpec, Serde, Serde)} or {@link StreamGraph#createIntStream(StreamSpec, Serde, Serde)}.
   * Hence, the proper types of serdes for key and value are instantiated and are used in the {@link SinkFunction} returned.
   *
   * @return  The pre-defined {@link SinkFunction} to apply proper serdes before sending the message to the output stream.
   */
  SinkFunction<M> getSinkFunction();

  StreamSpec getSpec();
}
