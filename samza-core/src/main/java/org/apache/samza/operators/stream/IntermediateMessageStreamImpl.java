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
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.system.StreamSpec;

/**
 * An intermediate stream is both an input and an output stream (e.g. a repartitioned stream).
 * <p>
 * This implementation accepts a pair of {@link InputOperatorSpec} and {@link OutputStreamImpl} associated
 * with the same logical {@code streamId}. It provides access to its {@link OutputStreamImpl} for
 * {@link MessageStreamImpl#partitionBy} to send messages out to. It's also a {@link MessageStreamImpl} with
 * {@link InputOperatorSpec} as its operator spec, so that further operations can be chained on the
 * {@link InputOperatorSpec}.
 *
 * @param <K> the type of key in the outgoing/incoming message
 * @param <V> the type of message in the outgoing/incoming message
 * @param <M> the type of message in the output {@link MessageStreamImpl}
 */
public class IntermediateMessageStreamImpl<K, V, M> extends MessageStreamImpl<M> implements OutputStream<K, V, M> {

  private final OutputStreamImpl<K, V, M> outputStream;

  public IntermediateMessageStreamImpl(StreamGraphImpl graph, InputOperatorSpec<K, V, M> inputOperatorSpec,
      OutputStreamImpl<K, V, M> outputStream) {
    super(graph, inputOperatorSpec);
    this.outputStream = outputStream;
  }

  public StreamSpec getStreamSpec() {
    return this.outputStream.getStreamSpec();
  }

  public OutputStreamImpl<K, V, M> getOutputStream() {
    return this.outputStream;
  }
}
