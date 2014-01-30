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

package org.apache.samza.system;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * SystemConsumer is the interface that must be implemented by any system that
 * wishes to integrate with Samza. Examples of systems that one might want to
 * integrate would be systems like Kafka, Hadoop, Kestrel, SQS, etc.
 * </p>
 * 
 * <p>
 * SamzaContainer uses SystemConsumer to read messages from the underlying
 * system, and funnels the messages to the appropriate StreamTask instances. The
 * basic flow is for the SamzaContainer to poll for all SystemStreamPartitions,
 * feed all IncomingMessageEnvelopes to the appropriate StreamTask, and then
 * repeat. If no IncomingMessageEnvelopes are returned, the SamzaContainer polls
 * again, but with a timeout of 10ms.
 * </p>
 * 
 * <p>
 * The SamzaContainer treats SystemConsumer in the following way:
 * </p>
 * 
 * <ul>
 * <li>Start will be called before stop.</li>
 * <li>Register will be called one or more times before start.</li>
 * <li>Register won't be called twice for the same SystemStreamPartition.</li>
 * <li>If timeout < 0, poll will block unless all SystemStreamPartition are at
 * "head" (the underlying system has been checked, and returned an empty set).
 * If at head, an empty list is returned.</li>
 * <li>If timeout >= 0, poll will return any messages that are currently
 * available for any of the SystemStreamPartitions specified. If no new messages
 * are available, it will wait up to timeout milliseconds for messages from any
 * SystemStreamPartition to become available. It will return an empty list if
 * the timeout is hit, and no new messages are available.</li>
 * <li>Nothing will be called after stop has been invoked.</li>
 * <li>Poll will only be called for registered SystemStreamPartition.</li>
 * <li>The SystemConsumer can't assume that a given SystemStreamPartition's
 * messages will ever be read. It shouldn't run out of memory or deadlock all
 * new message arrivals if one SystemStreamPartition is never read from.</li>
 * <li>Any exception thrown by the SystemConsumer means that the SamzaContainer
 * should halt.</li>
 * </ul>
 * 
 * <p>
 * There are generally three implementation styles to this interface:
 * </p>
 * 
 * <ol>
 * <li>Thread-based</li>
 * <li>Selector-based</li>
 * <li>Synchronous</li>
 * </ol>
 * 
 * <p>
 * Thread-based implementations typically use a series of threads to read from
 * an underlying system asynchronously, and put the resulting messages into a
 * queue, which is then read from whenever the poll method is invoked. The poll
 * method's parameters map very closely to Java's BlockingQueue interface.
 * BlockingEnvelopeMap is a helper class that makes it easy to implement
 * thread-based implementations of SystemConsumer.
 * </p>
 * 
 * <p>
 * Selector-based implementations typically setup NIO-based non-blocking socket
 * that can be selected for new data when poll is called.
 * </p>
 * 
 * <p>
 * Synchronous implementations simply fetch directly from the underlying system
 * whenever poll is invoked. Synchronous implementations must take great care to
 * adhere to the timeout rules defined in the poll method.
 * </p>
 */
public interface SystemConsumer {
  /**
   * A constant that can be used in the poll method's timeout parameter to
   * denote that the poll invocation should block until at least one message is
   * available for one of the SystemStreamPartitions supplied, or until all
   * SystemStreamPartitions supplied are at head (have no new messages available
   * since the last poll invocation was made for each SystemStreamPartition).
   */
  public static int BLOCK_ON_OUTSTANDING_MESSAGES = -1;

  /**
   * Tells the SystemConsumer to connect to the underlying system, and prepare
   * to begin serving messages when poll is invoked.
   */
  void start();

  /**
   * Tells the SystemConsumer to close all connections, release all resource,
   * and shut down everything. The SystemConsumer will not be used again after
   * stop is called.
   */
  void stop();

  /**
   * Register a SystemStreamPartition to this SystemConsumer. The SystemConsumer
   * should try and read messages from all SystemStreamPartitions that are
   * registered to it. SystemStreamPartitions should only be registered before
   * start is called.
   * 
   * @param systemStreamPartition
   *          The SystemStreamPartition object representing the Samza
   *          SystemStreamPartition to receive messages from.
   * @param lastReadOffset
   *          String representing the offset of the last message that was
   *          successfully processed by the SamzaContainer before the
   *          SamzaContainer was shut down. This allows the SystemConsumer to
   *          pick up where it left off in cases where the SamzaContainer fails,
   *          or is shut down and restarted. The lastReadOffset is saved and
   *          restored by the CheckpointManager whenever SamzaContainer shuts
   *          down and starts up.
   */
  void register(SystemStreamPartition systemStreamPartition, String lastReadOffset);

  /**
   * Poll the SystemConsumer to get any available messages from the underlying
   * system.
   * 
   * If the underlying implementation does not take care to adhere to the
   * timeout parameter, the SamzaContainer's performance will suffer
   * drastically. Specifically, if poll blocks when it's not supposed to, it
   * will block the entire main thread in SamzaContainer, and no messages will
   * be processed while blocking is occurring.
   * 
   * @param systemStreamPartitions
   *          A map from SystemStreamPartition to maximum number of messages to
   *          return for the SystemStreamPartition. Polling with {stream1: 100,
   *          stream2: 1000} tells the SystemConsumer that it can return between
   *          0 and 100 messages (inclusive) for stream1, and between 0 and 1000
   *          messages for stream2. If SystemConsumer has messages available for
   *          other registered SystemStreamPartitions, but they are not in the
   *          systemStreamPartitions map in a given poll invocation, they can't
   *          be returned. It is illegal to pass in SystemStreamPartitions that
   *          have not been registered with the SystemConsumer first.
   * @param timeout
   *          If timeout < 0, poll will block unless all SystemStreamPartition
   *          are at "head" (the underlying system has been checked, and
   *          returned an empty set). If at head, an empty list is returned. If
   *          timeout >= 0, poll will return any messages that are currently
   *          available for any of the SystemStreamPartitions specified. If no
   *          new messages are available, it will wait up to timeout
   *          milliseconds for messages from any SystemStreamPartition to become
   *          available. It will return an empty list if the timeout is hit, and
   *          no new messages are available.
   * @return A list of zero or more IncomingMessageEnvelopes for the
   *         SystemStreamPartitions that were supplied during the invocation.
   * @throws InterruptedException
   */
  List<IncomingMessageEnvelope> poll(Map<SystemStreamPartition, Integer> systemStreamPartitions, long timeout) throws InterruptedException;
}
