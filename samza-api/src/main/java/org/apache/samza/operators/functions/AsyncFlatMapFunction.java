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
package org.apache.samza.operators.functions;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CompletionStage;
import org.apache.samza.SamzaException;
import org.apache.samza.annotation.InterfaceStability;


/**
 * Asynchronous variant of the {@link FlatMapFunction} used in tandem with {@link org.apache.samza.operators.MessageStream#flatMapAsync(AsyncFlatMapFunction)}
 * to transform a collection of 0 or more messages.
 * <p>
 * Typically, {@link AsyncFlatMapFunction} is used for describing complex transformations that involve IO operations or remote calls.
 * The following pseudo code demonstrates a sample implementation of {@link AsyncFlatMapFunction} which transforms the
 * the message to decorated message which involves talking to a remote service.
 * <pre> {@code
 *    AsyncFlatMapFunction<M, OM> asyncRestDecoratorFunction = (M message) -> {
 *        ...
 *
 *        Request<DecoratedData> decoratorRequest = buildDecoratorRequest(message);
 *        Future<DecoratorDataResponse> decorateResponseFuture = restServiceClient.sendRequest(decoratorRequest); // remote call to the rest service;
 *        ...
 *
 *        return new CompletableFuture<>(decorateResponseFuture)
 *             .thenApply(decoratedDataResponse -> massageDecoratorResponse(decoratedDataResponse);
 *    }
 * }
 * </pre>
 *
 * <p>
 *   The function needs to be thread safe in case of task.max.concurrency&gt;1. It also needs to coordinate any
 *   shared state since happens-before is not guaranteed between the messages delivered to the function. Refer to
 *   {@link org.apache.samza.operators.MessageStream#flatMapAsync(AsyncFlatMapFunction)} docs for more details on the modes
 *   and guarantees.
 *
 * <p>
 *   For each invocation, the {@link CompletionStage} returned by the function should be completed successfully/exceptionally
 *   within task.callback.timeout.ms; failure to do so will result in {@link SamzaException} bringing down the application.
 *
 * @param <M>  type of the input message
 * @param <OM>  type of the transformed messages
 */
@InterfaceStability.Unstable
@FunctionalInterface
public interface AsyncFlatMapFunction<M, OM> extends InitableFunction, ClosableFunction, Serializable {

  /**
   * Transforms the provided message into a collection of 0 or more messages.
   *
   * @param message  the input message to be transformed
   * @return  a {@link CompletionStage} of a {@link Collection} of transformed messages
   */
  CompletionStage<Collection<OM>> apply(M message);
}
