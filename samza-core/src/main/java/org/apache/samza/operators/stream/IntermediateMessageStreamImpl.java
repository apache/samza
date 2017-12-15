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
package org.apache.samza.operators.stream;

import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.system.StreamSpec;

/**
 * An intermediate stream is both an input and an output stream (e.g. a repartitioned stream).
 * <p>
 * This implementation accepts a pair of {@link OperatorSpec} and {@link OutputStreamImpl} associated
 * with the same logical {@code streamId}. It provides access to its {@link OutputStreamImpl} for
 * the partitionBy operator to send messages out to. It's also a {@link MessageStreamImpl} with
 * {@link OperatorSpec} as its input operator node, so that further operations can be chained on the
 * {@link OperatorSpec}.
 *
 * @param <M> the type of message in the output {@link MessageStreamImpl}
 */
public class IntermediateMessageStreamImpl<M> extends MessageStreamImpl<M> implements OutputStream<M> {

  private final OutputStreamImpl<M> outputStream;

  public IntermediateMessageStreamImpl(StreamGraphImpl graph, OperatorSpec<?, M> inputOperatorNode,
      OutputStreamImpl<M> outputStream) {
    super(graph, inputOperatorNode);
    this.outputStream = outputStream;
  }

  public StreamSpec getStreamSpec() {
    return this.outputStream.getStreamSpec();
  }

  public OutputStreamImpl<M> getOutputStream() {
    return this.outputStream;
  }
}
