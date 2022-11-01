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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.context.InternalTaskContext;
import org.apache.samza.context.TaskContext;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.Scheduler;
import org.apache.samza.operators.functions.ScheduledFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.scheduler.CallbackScheduler;
import org.apache.samza.system.DrainMessage;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.HighResolutionClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
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
  // drain states
  private DrainStates drainStates;
  // watermark states
  private WatermarkStates watermarkStates;
  private CallbackScheduler callbackScheduler;
  private ControlMessageSender controlMessageSender;
  private int elasticityFactor;

  /**
   * Initialize this {@link OperatorImpl} and its user-defined functions.
   *
   * @param internalTaskContext the {@link InternalTaskContext} for the task
   */
  public final void init(InternalTaskContext internalTaskContext) {
    final Context context = internalTaskContext.getContext();

    String opId = getOpImplId();

    if (initialized) {
      throw new IllegalStateException(String.format("Attempted to initialize Operator %s more than once.", opId));
    }

    if (closed) {
      throw new IllegalStateException(String.format("Attempted to initialize Operator %s after it was closed.", opId));
    }

    this.highResClock = createHighResClock(context.getJobContext().getConfig());
    registeredOperators = new LinkedHashSet<>();
    prevOperators = new LinkedHashSet<>();
    inputStreams = new LinkedHashSet<>();

    final ContainerContext containerContext = context.getContainerContext();
    final MetricsRegistry metricsRegistry = containerContext.getContainerMetricsRegistry();
    this.numMessage = metricsRegistry.newCounter(METRICS_GROUP, opId + "-messages");
    this.handleMessageNs = metricsRegistry.newTimer(METRICS_GROUP, opId + "-handle-message-ns");
    this.handleTimerNs = metricsRegistry.newTimer(METRICS_GROUP, opId + "-handle-timer-ns");

    final TaskContext taskContext =  context.getTaskContext();
    this.taskName = taskContext.getTaskModel().getTaskName();
    this.eosStates = (EndOfStreamStates) internalTaskContext.fetchObject(EndOfStreamStates.class.getName());
    this.watermarkStates = (WatermarkStates) internalTaskContext.fetchObject(WatermarkStates.class.getName());
    this.drainStates = (DrainStates) internalTaskContext.fetchObject(DrainStates.class.getName());

    this.controlMessageSender = new ControlMessageSender(internalTaskContext.getStreamMetadataCache());
    this.taskModel = taskContext.getTaskModel();
    this.callbackScheduler = taskContext.getCallbackScheduler();
    handleInit(context);
    this.elasticityFactor = new JobConfig(context.getJobContext().getConfig()).getElasticityFactor();

    initialized = true;
  }

  /**
   * Initialize this {@link OperatorImpl} and its user-defined functions.
   *
   * @param context the {@link Context} for the task
   */
  protected abstract void handleInit(Context context);

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

  public final CompletionStage<Void> onMessageAsync(M message, MessageCollector collector,
      TaskCoordinator coordinator) {
    this.numMessage.inc();
    long startNs = this.highResClock.nanoTime();
    CompletionStage<Collection<RM>> completableResultsFuture;
    try {
      completableResultsFuture = handleMessageAsync(message, collector, coordinator);
    } catch (ClassCastException e) {
      String actualType = e.getMessage().replaceFirst(" cannot be cast to .*", "");
      String expectedType = e.getMessage().replaceFirst(".* cannot be cast to ", "");
      throw new SamzaException(
          String.format("Error applying operator %s (created at %s) to its input message. "
                  + "Expected input message to be of type %s, but found it to be of type %s. "
                  + "Are Serdes for the inputs to this operator configured correctly?",
              getOpImplId(), getOperatorSpec().getSourceLocation(), expectedType, actualType), e);
    }

    CompletionStage<Void> result = completableResultsFuture.thenCompose(results -> {
      long endNs = this.highResClock.nanoTime();
      this.handleMessageNs.update(endNs - startNs);

      return CompletableFuture.allOf(results.stream()
          .flatMap(r -> this.registeredOperators.stream()
            .map(op -> op.onMessageAsync(r, collector, coordinator)))
          .toArray(CompletableFuture[]::new));
    });

    WatermarkFunction watermarkFn = getOperatorSpec().getWatermarkFn();
    if (watermarkFn != null) {
      // check whether there is new watermark emitted from the user function
      Long outputWm = watermarkFn.getOutputWatermark();
      return result.thenCompose(ignored -> propagateWatermark(outputWm, collector, coordinator));
    }

    return result;
  }

  /**
   * Handle the incoming {@code message} asynchronously and return a {@link CompletionStage} of the results to be propagated
   * to the registered operators.
   *
   * @param message the input message
   * @param collector the {@link MessageCollector} in the context
   * @param coordinator the {@link TaskCoordinator} in the context
   *
   * @return a {@code CompletionStage} of the results of the transformation
   */
  protected abstract CompletionStage<Collection<RM>> handleMessageAsync(M message, MessageCollector collector,
      TaskCoordinator coordinator);

  /**
   * Handle timer ticks for this {@link OperatorImpl} and propagate the results and timer tick to registered operators.
   * <p>
   * Delegates to {@link #handleTimer(MessageCollector, TaskCoordinator)} for handling the timer tick.
   *
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   */
  public final CompletionStage<Void> onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    long startNs = this.highResClock.nanoTime();
    Collection<RM> results = handleTimer(collector, coordinator);
    long endNs = this.highResClock.nanoTime();
    this.handleTimerNs.update(endNs - startNs);

    CompletionStage<Void> resultFuture = CompletableFuture.allOf(
        results.stream()
            .flatMap(r -> this.registeredOperators.stream()
                .map(op -> op.onMessageAsync(r, collector, coordinator)))
            .toArray(CompletableFuture[]::new));

    return resultFuture.thenCompose(x ->
        CompletableFuture.allOf(this.registeredOperators
            .stream()
            .map(op -> op.onTimer(collector, coordinator))
            .toArray(CompletableFuture[]::new)));
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
   * returns true if current task should broadcast control message (end of stream/watermark) to others
   * if elasticity is not enabled (elasticity factor <=1 ) then the current task is eligible
   * if elastiicty is enabled, pick the elastic task consuming keybucket = 0 of the ssp as the eligible task
   * @param ssp ssp that the current task consumes
   * @return true if current task is eligible to broadcast control messages
   */
  private boolean shouldTaskBroadcastToOtherPartitions(SystemStreamPartition ssp) {
    if (elasticityFactor <= 1) {
      return true;
    }

    // if elasticity is enabled then taskModel actually has ssp with keybuckets in it
    // check if this current elastic task processes the first keybucket (=0) of the ssp given
    return
        taskModel.getSystemStreamPartitions().stream()
            .filter(sspInModel ->
                ssp.getSystemStream().equals(sspInModel.getSystemStream()) // ensure same systemstream as ssp given
                && ssp.getPartition().equals(sspInModel.getPartition()) // ensure same partition as ssp given
                && sspInModel.getKeyBucket() == 0) // ensure sspInModel has keyBucket 0
            .count() > 0; // >0 means current task consumes the keyBucket = 0 of the ssp given
  }

  /**
   * Aggregate {@link EndOfStreamMessage} from each ssp of the stream.
   * Invoke onEndOfStream() if the stream reaches the end.
   * @param eos {@link EndOfStreamMessage} object
   * @param ssp system stream partition
   * @param collector message collector
   * @param coordinator task coordinator
   */
  public final CompletionStage<Void> aggregateEndOfStream(EndOfStreamMessage eos, SystemStreamPartition ssp, MessageCollector collector,
      TaskCoordinator coordinator) {
    LOG.info("Received end-of-stream message from task {} in {}", eos.getTaskName(), ssp);
    eosStates.update(eos, ssp);

    SystemStream stream = ssp.getSystemStream();
    CompletionStage<Void> endOfStreamFuture = CompletableFuture.completedFuture(null);

    if (eosStates.isEndOfStream(stream)) {
      LOG.info("Input {} reaches the end for task {}", stream.toString(), taskName.getTaskName());
      if (eos.getTaskName() != null && shouldTaskBroadcastToOtherPartitions(ssp)) {
        // This is the aggregation task, which already received all the eos messages from upstream
        // broadcast the end-of-stream to all the peer partitions
        // additionally if elasiticty is enabled
        // then only one of the elastic tasks of the ssp will broadcast
        controlMessageSender.broadcastToOtherPartitions(new EndOfStreamMessage(), ssp, collector);
      }

      // populate the end-of-stream through the dag
      endOfStreamFuture = onEndOfStream(collector, coordinator)
          .thenAccept(result -> {
            if (eosStates.allEndOfStream()) {
              // all inputs have been end-of-stream, shut down the task
              LOG.info("All input streams have reached the end for task {}", taskName.getTaskName());
              coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
              coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
            }
          });
    }

    return endOfStreamFuture;
  }

  /**
   * Invoke handleEndOfStream() if all the input streams to the current operator reach the end.
   * Propagate the end-of-stream to downstream operators.
   * @param collector message collector
   * @param coordinator task coordinator
   */
  private CompletionStage<Void> onEndOfStream(MessageCollector collector, TaskCoordinator coordinator) {
    CompletionStage<Void> endOfStreamFuture = CompletableFuture.completedFuture(null);

    if (inputStreams.stream().allMatch(input -> eosStates.isEndOfStream(input))) {
      Collection<RM> results = handleEndOfStream(collector, coordinator);

      CompletionStage<Void> resultFuture = CompletableFuture.allOf(
          results.stream()
              .flatMap(r -> this.registeredOperators.stream()
                  .map(op -> op.onMessageAsync(r, collector, coordinator)))
              .toArray(CompletableFuture[]::new));

      endOfStreamFuture = resultFuture.thenCompose(x ->
          CompletableFuture.allOf(this.registeredOperators.stream()
              .map(op -> op.onEndOfStream(collector, coordinator))
              .toArray(CompletableFuture[]::new)));
    }

    return endOfStreamFuture;
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
   * This method is implemented when all input stream to this operation have encountered drain control message.
   * Inherited operator implementation should handle drain by overriding this function.
   * By default, noop implementation is for in-memory operator to handle the drain. Output operator need to
   * override this to actually propagate drain control message over the wire.
   * @param collector message collector
   * @param coordinator task coordinator
   * @return results to be emitted when this operator encounters drain control message
   */
  protected Collection<RM> handleDrain(MessageCollector collector, TaskCoordinator coordinator) {
    return Collections.emptyList();
  }

  /**
   * Aggregate {@link DrainMessage} from each ssp of the stream.
   * Invoke {@link #onDrainOfStream(MessageCollector, TaskCoordinator)} if the stream reaches the end.
   * @param drainMessage {@link DrainMessage} object
   * @param ssp system stream partition
   * @param collector message collector
   * @param coordinator task coordinator
   */
  public final CompletionStage<Void> aggregateDrainMessages(DrainMessage drainMessage, SystemStreamPartition ssp,
      MessageCollector collector, TaskCoordinator coordinator) {
    LOG.info("Received drain message from task {} in {}", drainMessage.getTaskName(), ssp);
    drainStates.update(drainMessage, ssp);

    SystemStream stream = ssp.getSystemStream();
    CompletionStage<Void> drainFuture = CompletableFuture.completedFuture(null);

    if (drainStates.isDrained(stream)) {
      LOG.info("Input {} is drained for task {}", stream.toString(), taskName.getTaskName());
      if (drainMessage.getTaskName() != null) {
        // This is the aggregation task which already received all the drain messages from upstream.
        // Broadcast the drain messages to all the peer partitions.
        controlMessageSender.broadcastToOtherPartitions(new DrainMessage(drainMessage.getRunId()), ssp, collector);
      }

      drainFuture = onDrainOfStream(collector, coordinator)
          .thenAccept(result -> {
            if (drainStates.areAllStreamsDrained()) {
              // All input streams have been drained, shut down the task
              LOG.info("All input streams have been drained for task {}. Requesting shutdown.", taskName.getTaskName());
              coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
              coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
            }
          });
    }

    return drainFuture;
  }


  /**
   * Invoke {@link #handleDrain(MessageCollector, TaskCoordinator)} if all the input streams to the current operator
   * have encountered drain message.
   * Propagate the drain to downstream operators.
   * @param collector message collector
   * @param coordinator task coordinator
   */
  private CompletionStage<Void> onDrainOfStream(MessageCollector collector, TaskCoordinator coordinator) {
    CompletionStage<Void> drainFuture = CompletableFuture.completedFuture(null);
    if (inputStreams.stream().allMatch(input -> drainStates.isDrained(input))) {
      Collection<RM> results = handleDrain(collector, coordinator);
      CompletionStage<Void> resultFuture = CompletableFuture.allOf(
          results.stream()
              .flatMap(r -> this.registeredOperators.stream()
                  .map(op -> op.onMessageAsync(r, collector, coordinator)))
              .toArray(CompletableFuture[]::new));

      // propagate DrainMessage to downstream operators
      drainFuture = resultFuture.thenCompose(x ->
          CompletableFuture.allOf(this.registeredOperators.stream()
              .map(op -> op.onDrainOfStream(collector, coordinator))
              .toArray(CompletableFuture[]::new)));
    }

    return drainFuture;
  }

  /**
   * Aggregate the {@link WatermarkMessage} from each ssp into a watermark. Then call onWatermark() if
   * a new watermark exits.
   * @param watermarkMessage a {@link WatermarkMessage} object
   * @param ssp {@link SystemStreamPartition} that the message is coming from.
   * @param collector message collector
   * @param coordinator task coordinator
   */
  public final CompletionStage<Void> aggregateWatermark(WatermarkMessage watermarkMessage, SystemStreamPartition ssp,
      MessageCollector collector, TaskCoordinator coordinator) {
    LOG.debug("Received watermark {} from {}", watermarkMessage.getTimestamp(), ssp);
    watermarkStates.update(watermarkMessage, ssp);
    long watermark = watermarkStates.getWatermark(ssp.getSystemStream());
    CompletionStage<Void> watermarkFuture = CompletableFuture.completedFuture(null);

    if (currentWatermark < watermark) {
      LOG.debug("Got watermark {} from stream {}", watermark, ssp.getSystemStream());

      if (watermarkMessage.getTaskName() != null && shouldTaskBroadcastToOtherPartitions(ssp)) {
        // This is the aggregation task, which already received all the watermark messages from upstream
        // broadcast the watermark to all the peer partitions
        // additionally if elasiticty is enabled
        // then only one of the elastic tasks of the ssp will broadcast
        controlMessageSender.broadcastToOtherPartitions(new WatermarkMessage(watermark), ssp, collector);
      }
      // populate the watermark through the dag
      watermarkFuture = onWatermark(watermark, collector, coordinator)
          .thenAccept(ignored -> watermarkStates.updateAggregateMetric(ssp, watermark));
    }

    return watermarkFuture;
  }

  /**
   * A watermark comes from an upstream operator. This function decides whether we should update the
   * input watermark based on the watermark time of all the previous operators, and then call handleWatermark()
   * to let the inherited operator to act on it.
   * @param watermark incoming watermark from an upstream operator
   * @param collector message collector
   * @param coordinator task coordinator
   */
  private CompletionStage<Void> onWatermark(long watermark, MessageCollector collector, TaskCoordinator coordinator) {
    final long inputWatermarkMin;
    if (prevOperators.isEmpty()) {
      // for input operator, use the watermark time coming from the source input
      inputWatermarkMin = watermark;
    } else {
      // InputWatermark(op) = min { OutputWatermark(op') | op' is upstream of op}
      inputWatermarkMin = prevOperators.stream()
          .map(op -> op.getOutputWatermark())
          .min(Long::compare)
          .get();
    }

    CompletionStage<Void> watermarkFuture = CompletableFuture.completedFuture(null);
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
        watermarkFuture = CompletableFuture.allOf(
            output.stream()
                .flatMap(rm -> this.registeredOperators.stream()
                    .map(op -> op.onMessageAsync(rm, collector, coordinator)))
                .toArray(CompletableFuture[]::new));
      }

      watermarkFuture = watermarkFuture.thenCompose(res -> propagateWatermark(outputWm, collector, coordinator));
    }

    return watermarkFuture;
  }

  private CompletionStage<Void> propagateWatermark(Long outputWm, MessageCollector collector, TaskCoordinator coordinator) {
    CompletionStage<Void> watermarkFuture = CompletableFuture.completedFuture(null);

    if (outputWm != null) {
      if (outputWatermark < outputWm) {
        // advance the watermark
        outputWatermark = outputWm;
        LOG.debug("Advance output watermark to {} in operator {}", outputWatermark, getOpImplId());
        watermarkFuture = CompletableFuture.allOf(
            this.registeredOperators
                .stream()
                .map(op -> op.onWatermark(outputWatermark, collector, coordinator))
                .toArray(CompletableFuture[]::new));
      } else if (outputWatermark > outputWm) {
        LOG.warn("Ignore watermark {} that is smaller than the previous watermark {}.", outputWm, outputWatermark);
      }
    }

    return watermarkFuture;
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
        callbackScheduler.scheduleCallback(key, time, (k, collector, coordinator) -> {
          final ScheduledFunction<K, RM> scheduledFn = getOperatorSpec().getScheduledFn();
          if (scheduledFn != null) {
            final Collection<RM> output = scheduledFn.onCallback(key, time);

            if (!output.isEmpty()) {
              CompletableFuture<Void> timerFuture = CompletableFuture.allOf(output.stream()
                  .flatMap(r -> registeredOperators.stream()
                      .map(op -> op.onMessageAsync(r, collector, coordinator)))
                  .toArray(CompletableFuture[]::new));

              timerFuture.join();
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
        callbackScheduler.deleteCallback(key);
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

  /* Package Private helper method for tests to perform onMessage synchronously
   * Note: It is only intended for test use
   */
  @VisibleForTesting
  final void onMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    onMessageAsync(message, collector, coordinator)
        .toCompletableFuture().join();
  }

  /* Package Private helper method for tests to perform handleMessage synchronously
   * Note: It is only intended for test use
   */
  @VisibleForTesting
  final Collection<RM> handleMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    return handleMessageAsync(message, collector, coordinator)
        .toCompletableFuture().join();
  }

  private HighResolutionClock createHighResClock(Config config) {
    MetricsConfig metricsConfig = new MetricsConfig(config);
    // The timer metrics calculation here is only enabled for debugging
    if (metricsConfig.getMetricsTimerEnabled()
        && metricsConfig.getMetricsTimerDebugEnabled()) {
      return System::nanoTime;
    } else {
      return () -> 0;
    }
  }
}
