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
package org.apache.samza.operators.impl;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.container.TaskContextImpl;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.operators.Scheduler;
import org.apache.samza.operators.functions.ScheduledFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.HighResolutionClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * Abstract base class for all stream operator implementations.
 *
 * @param <M> type of the input to this operator
 * @param <RM> type of the results of applying this operator
 */
public abstract class OperatorImpl<M, RM> {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorImpl.class);
  private static final String METRICS_GROUP = OperatorImpl.class.getName();

  private boolean initialized;
  private boolean closed;
  private HighResolutionClock highResClock;
  private Counter numMessage;
  private Timer handleMessageNs;
  private Timer handleTimerNs;
  private long currentWatermark = WatermarkStates.WATERMARK_NOT_EXIST;
  private long outputWatermark = WatermarkStates.WATERMARK_NOT_EXIST;
  private TaskName taskName;
  // Although the operator node is in the operator graph, the current task may not consume any message in it.
  // This can be caused by none of the input stream partitions of this op is assigned to the current task.
  // It's important to know so we can populate the watermarks correctly.
  private boolean usedInCurrentTask = false;

  Set<OperatorImpl<RM, ?>> registeredOperators;
  Set<OperatorImpl<?, M>> prevOperators;
  Set<SystemStream> inputStreams;

  private TaskModel taskModel;
  // end-of-stream states
  private EndOfStreamStates eosStates;
  // watermark states
  private WatermarkStates watermarkStates;
  private TaskContext taskContext;
  private ControlMessageSender controlMessageSender;

  /**
   * Initialize this {@link OperatorImpl} and its user-defined functions.
   *
   * @param config  the {@link Config} for the task
   * @param context  the {@link TaskContext} for the task
   */
  public final void init(Config config, TaskContext context) {
    String opId = getOpImplId();

    if (initialized) {
      throw new IllegalStateException(String.format("Attempted to initialize Operator %s more than once.", opId));
    }

    if (closed) {
      throw new IllegalStateException(String.format("Attempted to initialize Operator %s after it was closed.", opId));
    }

    this.highResClock = createHighResClock(config);
    registeredOperators = new HashSet<>();
    prevOperators = new HashSet<>();
    inputStreams = new HashSet<>();
    MetricsRegistry metricsRegistry = context.getMetricsRegistry();
    this.numMessage = metricsRegistry.newCounter(METRICS_GROUP, opId + "-messages");
    this.handleMessageNs = metricsRegistry.newTimer(METRICS_GROUP, opId + "-handle-message-ns");
    this.handleTimerNs = metricsRegistry.newTimer(METRICS_GROUP, opId + "-handle-timer-ns");
    this.taskName = context.getTaskName();

    TaskContextImpl taskContext = (TaskContextImpl) context;
    this.eosStates = (EndOfStreamStates) taskContext.fetchObject(EndOfStreamStates.class.getName());
    this.watermarkStates = (WatermarkStates) taskContext.fetchObject(WatermarkStates.class.getName());
    this.controlMessageSender = new ControlMessageSender(taskContext.getStreamMetadataCache());

    if (taskContext.getJobModel() != null) {
      ContainerModel containerModel = taskContext.getJobModel().getContainers()
          .get(context.getSamzaContainerContext().id);
      this.taskModel = containerModel.getTasks().get(taskName);
    } else {
      this.taskModel = null;
      this.usedInCurrentTask = true;
    }

    this.taskContext = taskContext;
    handleInit(config, taskContext);

    initialized = true;
  }

  /**
   * Initialize this {@link OperatorImpl} and its user-defined functions.
   *
   * @param config  the {@link Config} for the task
   * @param context  the {@link TaskContext} for the task
   */
  protected abstract void handleInit(Config config, TaskContext context);

  /**
   * Register an operator that this operator should propagate its results to.
   *
   * @param nextOperator  the next operator to propagate results to
   */
  void registerNextOperator(OperatorImpl<RM, ?> nextOperator) {
    if (!initialized) {
      throw new IllegalStateException(
          String.format("Attempted to register next operator before initializing operator %s.",
              getOpImplId()));
    }
    this.registeredOperators.add(nextOperator);
    nextOperator.registerPrevOperator(this);
  }

  void registerPrevOperator(OperatorImpl<?, M> prevOperator) {
    this.prevOperators.add(prevOperator);
  }

  void registerInputStream(SystemStream input) {
    this.inputStreams.add(input);

    usedInCurrentTask = usedInCurrentTask
        || taskModel.getSystemStreamPartitions().stream().anyMatch(ssp -> ssp.getSystemStream().equals(input));
  }

  /**
   * Handle the incoming {@code message} for this {@link OperatorImpl} and propagate results to registered operators.
   * <p>
   * Delegates to {@link #handleMessage(Object, MessageCollector, TaskCoordinator)} for handling the message.
   *
   * @param message  the input message
   * @param collector  the {@link MessageCollector} for this message
   * @param coordinator  the {@link TaskCoordinator} for this message
   */
  public final void onMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    this.numMessage.inc();
    long startNs = this.highResClock.nanoTime();
    Collection<RM> results;
    try {
      results = handleMessage(message, collector, coordinator);
    } catch (ClassCastException e) {
      String actualType = e.getMessage().replaceFirst(" cannot be cast to .*", "");
      String expectedType = e.getMessage().replaceFirst(".* cannot be cast to ", "");
      throw new SamzaException(
          String.format("Error applying operator %s (created at %s) to its input message. "
                  + "Expected input message to be of type %s, but found it to be of type %s. "
                  + "Are Serdes for the inputs to this operator configured correctly?",
              getOpImplId(), getOperatorSpec().getSourceLocation(), expectedType, actualType), e);
    }

    long endNs = this.highResClock.nanoTime();
    this.handleMessageNs.update(endNs - startNs);

    results.forEach(rm ->
        this.registeredOperators.forEach(op ->
            op.onMessage(rm, collector, coordinator)));

    WatermarkFunction watermarkFn = getOperatorSpec().getWatermarkFn();
    if (watermarkFn != null) {
      // check whether there is new watermark emitted from the user function
      Long outputWm = watermarkFn.getOutputWatermark();
      propagateWatermark(outputWm, collector, coordinator);
    }
  }

  /**
   * Handle the incoming {@code message} and return the results to be propagated to registered operators.
   *
   * @param message  the input message
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   * @return  results of the transformation
   */
  protected abstract Collection<RM> handleMessage(M message, MessageCollector collector,
      TaskCoordinator coordinator);

  /**
   * Handle timer ticks for this {@link OperatorImpl} and propagate the results and timer tick to registered operators.
   * <p>
   * Delegates to {@link #handleTimer(MessageCollector, TaskCoordinator)} for handling the timer tick.
   *
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   */
  public final void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    long startNs = this.highResClock.nanoTime();
    Collection<RM> results = handleTimer(collector, coordinator);
    long endNs = this.highResClock.nanoTime();
    this.handleTimerNs.update(endNs - startNs);

    results.forEach(rm ->
        this.registeredOperators.forEach(op ->
            op.onMessage(rm, collector, coordinator)));
    this.registeredOperators.forEach(op ->
        op.onTimer(collector, coordinator));
  }

  /**
   * Handle the the timer tick for this operator and return the results to be propagated to registered operators.
   * <p>
   * Defaults to a no-op implementation.
   *
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   * @return  results of the timed operation
   */
  protected Collection<RM> handleTimer(MessageCollector collector, TaskCoordinator coordinator) {
    return Collections.emptyList();
  }

  /**
   * Aggregate {@link EndOfStreamMessage} from each ssp of the stream.
   * Invoke onEndOfStream() if the stream reaches the end.
   * @param eos {@link EndOfStreamMessage} object
   * @param ssp system stream partition
   * @param collector message collector
   * @param coordinator task coordinator
   */
  public final void aggregateEndOfStream(EndOfStreamMessage eos, SystemStreamPartition ssp, MessageCollector collector,
      TaskCoordinator coordinator) {
    LOG.info("Received end-of-stream message from task {} in {}", eos.getTaskName(), ssp);
    eosStates.update(eos, ssp);

    SystemStream stream = ssp.getSystemStream();
    if (eosStates.isEndOfStream(stream)) {
      LOG.info("Input {} reaches the end for task {}", stream.toString(), taskName.getTaskName());
      if (eos.getTaskName() != null) {
        // This is the aggregation task, which already received all the eos messages from upstream
        // broadcast the end-of-stream to all the peer partitions
        controlMessageSender.broadcastToOtherPartitions(new EndOfStreamMessage(), ssp, collector);
      }
      // populate the end-of-stream through the dag
      onEndOfStream(collector, coordinator);

      if (eosStates.allEndOfStream()) {
        // all inputs have been end-of-stream, shut down the task
        LOG.info("All input streams have reached the end for task {}", taskName.getTaskName());
        coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
        coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
      }
    }
  }

  /**
   * Invoke handleEndOfStream() if all the input streams to the current operator reach the end.
   * Propagate the end-of-stream to downstream operators.
   * @param collector message collector
   * @param coordinator task coordinator
   */
  private final void onEndOfStream(MessageCollector collector, TaskCoordinator coordinator) {
    if (inputStreams.stream().allMatch(input -> eosStates.isEndOfStream(input))) {
      Collection<RM> results = handleEndOfStream(collector, coordinator);

      results.forEach(rm ->
          this.registeredOperators.forEach(op ->
              op.onMessage(rm, collector, coordinator)));

      this.registeredOperators.forEach(op -> op.onEndOfStream(collector, coordinator));
    }
  }

  /**
   * All input streams to this operator reach to the end.
   * Inherited class should handle end-of-stream by overriding this function.
   * By default noop implementation is for in-memory operator to handle the EOS. Output operator need to
   * override this to actually propagate EOS over the wire.
   * @param collector message collector
   * @param coordinator task coordinator
   * @return results to be emitted when this operator reaches end-of-stream
   */
  protected Collection<RM> handleEndOfStream(MessageCollector collector, TaskCoordinator coordinator) {
    return Collections.emptyList();
  }

  /**
   * Aggregate the {@link WatermarkMessage} from each ssp into a watermark. Then call onWatermark() if
   * a new watermark exits.
   * @param watermarkMessage a {@link WatermarkMessage} object
   * @param ssp {@link SystemStreamPartition} that the message is coming from.
   * @param collector message collector
   * @param coordinator task coordinator
   */
  public final void aggregateWatermark(WatermarkMessage watermarkMessage, SystemStreamPartition ssp,
      MessageCollector collector, TaskCoordinator coordinator) {
    LOG.debug("Received watermark {} from {}", watermarkMessage.getTimestamp(), ssp);
    watermarkStates.update(watermarkMessage, ssp);
    long watermark = watermarkStates.getWatermark(ssp.getSystemStream());
    if (currentWatermark < watermark) {
      LOG.debug("Got watermark {} from stream {}", watermark, ssp.getSystemStream());

      if (watermarkMessage.getTaskName() != null) {
        // This is the aggregation task, which already received all the watermark messages from upstream
        // broadcast the watermark to all the peer partitions
        controlMessageSender.broadcastToOtherPartitions(new WatermarkMessage(watermark), ssp, collector);
      }
      // populate the watermark through the dag
      onWatermark(watermark, collector, coordinator);

      // update metrics
      watermarkStates.updateAggregateMetric(ssp, watermark);
    }
  }

  /**
   * A watermark comes from an upstream operator. This function decides whether we should update the
   * input watermark based on the watermark time of all the previous operators, and then call handleWatermark()
   * to let the inherited operator to act on it.
   * @param watermark incoming watermark from an upstream operator
   * @param collector message collector
   * @param coordinator task coordinator
   */
  private final void onWatermark(long watermark, MessageCollector collector, TaskCoordinator coordinator) {
    final long inputWatermarkMin;
    if (prevOperators.isEmpty()) {
      // for input operator, use the watermark time coming from the source input
      inputWatermarkMin = watermark;
    } else {
      // InputWatermark(op) = min { OutputWatermark(op') | op' is upstream of op}
      inputWatermarkMin = prevOperators.stream().map(op -> op.getOutputWatermark()).min(Long::compare).get();
    }

    if (currentWatermark < inputWatermarkMin) {
      // advance the watermark time of this operator
      currentWatermark = inputWatermarkMin;
      LOG.trace("Advance input watermark to {} in operator {}", currentWatermark, getOpImplId());

      final Long outputWm;
      final Collection<RM> output;
      final WatermarkFunction watermarkFn = getOperatorSpec().getWatermarkFn();
      if (watermarkFn != null) {
        // user-overrided watermark handling here
        output = (Collection<RM>) watermarkFn.processWatermark(currentWatermark);
        outputWm = watermarkFn.getOutputWatermark();
      } else {
        // use samza-provided watermark handling
        // default is to propagate the input watermark
        output = handleWatermark(currentWatermark, collector, coordinator);
        outputWm = currentWatermark;
      }

      if (!output.isEmpty()) {
        output.forEach(rm ->
            this.registeredOperators.forEach(op ->
                op.onMessage(rm, collector, coordinator)));
      }

      propagateWatermark(outputWm, collector, coordinator);
    }
  }

  private void propagateWatermark(Long outputWm, MessageCollector collector, TaskCoordinator coordinator) {
    if (outputWm != null) {
      if (outputWatermark < outputWm) {
        // advance the watermark
        outputWatermark = outputWm;
        LOG.debug("Advance output watermark to {} in operator {}", outputWatermark, getOpImplId());
        this.registeredOperators.forEach(op -> op.onWatermark(outputWatermark, collector, coordinator));
      } else if (outputWatermark > outputWm) {
        LOG.warn("Ignore watermark {} that is smaller than the previous watermark {}.", outputWm, outputWatermark);
      }
    }
  }

  /**
   * Handling of the input watermark and returns the output watermark.
   * In-memory operator can override this to fire event-time triggers. Output operators need to override it
   * so it can propagate watermarks over the wire. By default it simply returns the input watermark.
   * @param inputWatermark  input watermark
   * @param collector message collector
   * @param coordinator task coordinator
   * @return output watermark, or null if the output watermark should not be updated.
   */
  protected Collection<RM> handleWatermark(long inputWatermark, MessageCollector collector, TaskCoordinator coordinator) {
    // Default is no handling. Output is empty.
    return Collections.emptyList();
  }

  /* package private for testing */
  final long getInputWatermark() {
    return this.currentWatermark;
  }

  /**
   * Returns the output watermark,
   * @return output watermark
   */
  final long getOutputWatermark() {
    if (usedInCurrentTask) {
      // default as input
      return this.outputWatermark;
    } else {
      // always emit the max to indicate no input will be emitted afterwards
      return Long.MAX_VALUE;
    }
  }

  /**
   * Returns a registry which allows registering arbitrary system-clock timer with K-typed key.
   * The user-defined function in the operator spec needs to implement {@link ScheduledFunction#onCallback(Object, long)}
   * for timer notifications.
   * @param <K> key type for the timer.
   * @return an instance of {@link Scheduler}
   */
  <K> Scheduler<K> createOperatorScheduler() {
    return new Scheduler<K>() {
      @Override
      public void schedule(K key, long time) {
        taskContext.scheduleCallback(key, time, (k, collector, coordinator) -> {
            final ScheduledFunction<K, RM> scheduledFn = getOperatorSpec().getScheduledFn();
            if (scheduledFn != null) {
              final Collection<RM> output = scheduledFn.onCallback(key, time);

              if (!output.isEmpty()) {
                output.forEach(rm ->
                    registeredOperators.forEach(op ->
                        op.onMessage(rm, collector, coordinator)));
              }
            } else {
              throw new SamzaException(
                  String.format("Operator %s id %s (created at %s) must implement ScheduledFunction to use system timer.",
                      getOperatorSpec().getOpCode().name(), getOpImplId(), getOperatorSpec().getSourceLocation()));
            }
          });
      }

      @Override
      public void delete(K key) {
        taskContext.deleteScheduledCallback(key);
      }
    };
  }

  public void close() {
    if (closed) {
      throw new IllegalStateException(
          String.format("Attempted to close Operator %s more than once.", getOpImplId()));
    }
    handleClose();
    closed = true;
  }

  protected abstract void handleClose();

  /**
   * Get the {@link OperatorSpec} for this {@link OperatorImpl}.
   *
   * @return the {@link OperatorSpec} for this {@link OperatorImpl}
   */
  protected abstract OperatorSpec<M, RM> getOperatorSpec();

  /**
   * Get the unique ID for this {@link OperatorImpl} in the DAG.
   *
   * Some {@link OperatorImpl}s don't have a 1:1 mapping with their {@link OperatorSpec}. E.g., there are
   * 2 PartialJoinOperatorImpls for a JoinOperatorSpec. Overriding this method allows them to provide an
   * implementation specific id, e.g., for use in metrics.
   *
   * @return the unique ID for this {@link OperatorImpl} in the DAG
   */
  protected String getOpImplId() {
    return getOperatorSpec().getOpId();
  }

  private HighResolutionClock createHighResClock(Config config) {
    if (new MetricsConfig(config).getMetricsTimerEnabled()) {
      return System::nanoTime;
    } else {
      return () -> 0;
    }
  }
}
