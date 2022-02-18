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

package org.apache.samza.container;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.MessageType;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.CoordinatorRequests;
import org.apache.samza.scheduler.EpochTimeScheduler;
import org.apache.samza.task.ReadableCoordinator;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskCallbackFactory;
import org.apache.samza.task.TaskCallbackImpl;
import org.apache.samza.task.TaskCallbackListener;
import org.apache.samza.task.TaskCallbackManager;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.HighResolutionClock;
import org.apache.samza.util.Throttleable;
import org.apache.samza.util.ThrottlingScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The run loop supports both single-threaded and multi-threaded execution models.
 *    <p>
 *      If job.container.thread.pool.size &gt; 1 (multi-threaded), operations like commit, window and timer for all tasks within a container
 *      happens on a thread pool.
 *      If job.container.thread.pool.size &lt; 1 (single-threaded), operations for all tasks are multiplexed onto one execution thread.
 *    </p>.
 *    Note: In both models, process/processAsync for all tasks is invoked on the run loop thread.
 */
public class RunLoop implements Runnable, Throttleable {
  private static final Logger log = LoggerFactory.getLogger(RunLoop.class);

  private final List<AsyncTaskWorker> taskWorkers;
  private final SystemConsumers consumerMultiplexer;
  private final Map<SystemStreamPartition, List<AsyncTaskWorker>> sspToTaskWorkerMapping;

  private final ExecutorService threadPool;
  private final CoordinatorRequests coordinatorRequests;
  private final Object latch;
  private final int maxConcurrency;
  private final long windowMs;
  private final long commitMs;
  private final long callbackTimeoutMs;
  private final long maxIdleMs;
  private final SamzaContainerMetrics containerMetrics;
  private final ScheduledExecutorService workerTimer;
  private final ScheduledExecutorService callbackTimer;
  private final ThrottlingScheduler callbackExecutor;
  private volatile boolean shutdownNow = false;
  private volatile Throwable throwable = null;
  private final HighResolutionClock clock;
  private final boolean isAsyncCommitEnabled;
  private volatile boolean runLoopResumedSinceLastChecked;
  private final int elasticityFactor;

  public RunLoop(Map<TaskName, RunLoopTask> runLoopTasks,
      ExecutorService threadPool,
      SystemConsumers consumerMultiplexer,
      int maxConcurrency,
      long windowMs,
      long commitMs,
      long callbackTimeoutMs,
      long maxThrottlingDelayMs,
      long maxIdleMs,
      SamzaContainerMetrics containerMetrics,
      HighResolutionClock clock,
      boolean isAsyncCommitEnabled) {
    this(runLoopTasks, threadPool, consumerMultiplexer, maxConcurrency, windowMs, commitMs, callbackTimeoutMs,
        maxThrottlingDelayMs, maxIdleMs, containerMetrics, clock, isAsyncCommitEnabled, 1);
  }

  public RunLoop(Map<TaskName, RunLoopTask> runLoopTasks,
      ExecutorService threadPool,
      SystemConsumers consumerMultiplexer,
      int maxConcurrency,
      long windowMs,
      long commitMs,
      long callbackTimeoutMs,
      long maxThrottlingDelayMs,
      long maxIdleMs,
      SamzaContainerMetrics containerMetrics,
      HighResolutionClock clock,
      boolean isAsyncCommitEnabled,
      int elasticityFactor) {

    this.threadPool = threadPool;
    this.consumerMultiplexer = consumerMultiplexer;
    this.containerMetrics = containerMetrics;
    this.windowMs = windowMs;
    this.commitMs = commitMs;
    this.maxConcurrency = maxConcurrency;
    this.callbackTimeoutMs = callbackTimeoutMs;
    this.maxIdleMs = maxIdleMs;
    this.callbackTimer = (callbackTimeoutMs > 0) ? Executors.newSingleThreadScheduledExecutor() : null;
    this.callbackExecutor = new ThrottlingScheduler(maxThrottlingDelayMs);
    this.coordinatorRequests = new CoordinatorRequests(runLoopTasks.keySet());
    this.latch = new Object();
    this.workerTimer = Executors.newSingleThreadScheduledExecutor();
    this.clock = clock;
    Map<TaskName, AsyncTaskWorker> workers = new HashMap<>();
    for (RunLoopTask task : runLoopTasks.values()) {
      workers.put(task.taskName(), new AsyncTaskWorker(task));
    }
    // Partions and tasks assigned to the container will not change during the run loop life time
    this.sspToTaskWorkerMapping = Collections.unmodifiableMap(getSspToAsyncTaskWorkerMap(runLoopTasks, workers));
    this.taskWorkers = Collections.unmodifiableList(new ArrayList<>(workers.values()));
    this.isAsyncCommitEnabled = isAsyncCommitEnabled;
    this.elasticityFactor = elasticityFactor;
  }

  /**
   * Returns mapping of the SystemStreamPartition to the AsyncTaskWorkers to efficiently route the envelopes
   */
  private static Map<SystemStreamPartition, List<AsyncTaskWorker>> getSspToAsyncTaskWorkerMap(
      Map<TaskName, RunLoopTask> runLoopTasks, Map<TaskName, AsyncTaskWorker> taskWorkers) {
    Map<SystemStreamPartition, List<AsyncTaskWorker>> sspToWorkerMap = new HashMap<>();
    for (RunLoopTask task : runLoopTasks.values()) {
      Set<SystemStreamPartition> ssps = task.systemStreamPartitions();
      for (SystemStreamPartition ssp : ssps) {
        sspToWorkerMap.putIfAbsent(ssp, new ArrayList<>());
        sspToWorkerMap.get(ssp).add(taskWorkers.get(task.taskName()));
      }
    }
    return sspToWorkerMap;
  }

  /**
   * The run loop chooses messages from the SystemConsumers, and run the ready tasks asynchronously.
   * Window and commit are run in a thread pool, and they are mutual exclusive with task process.
   * The loop thread will block if all tasks are busy, and resume if any task finishes.
   */
  @Override
  public void run() {
    try {
      for (AsyncTaskWorker taskWorker : taskWorkers) {
        taskWorker.init();
      }

      long prevNs = clock.nanoTime();

      while (!shutdownNow && throwable == null) {
        long startNs = clock.nanoTime();

        IncomingMessageEnvelope envelope = chooseEnvelope();

        long chooseNs = clock.nanoTime();
        containerMetrics.chooseNs().update(chooseNs - startNs);

        blockIfBusyOrNoNewWork(envelope);

        long blockNs = clock.nanoTime();
        containerMetrics.blockNs().update(blockNs - chooseNs);

        runTasks(envelope);

        long currentNs = clock.nanoTime();
        long activeNs = currentNs - blockNs;
        long totalNs = currentNs - prevNs;
        prevNs = currentNs;

        if (totalNs != 0) {
          // totalNs is not 0 if timer metrics are enabled
          containerMetrics.utilization().set(((double) activeNs) / totalNs);
        }
      }

      /*
       * The current semantics of external shutdown request (RunLoop.shutdown()) is loosely defined and run loop doesn't
       * wait for inflight messages to complete and triggers shutdown as soon as it notices the shutdown request.
       * Hence, it is possible that the exception may or may not propagated based on order of execution
       * between process callback and run loop thread.
       */
      if (throwable != null) {
        log.error("Caught throwable and stopping run loop", throwable);
        throw new SamzaException(throwable);
      }
    } finally {
      workerTimer.shutdown();
      callbackExecutor.shutdown();
      if (callbackTimer != null) callbackTimer.shutdown();
    }
  }

  @Override
  public void setWorkFactor(double workFactor) {
    callbackExecutor.setWorkFactor(workFactor);
  }

  @Override
  public double getWorkFactor() {
    return callbackExecutor.getWorkFactor();
  }

  public void shutdown() {
    shutdownNow = true;
    resume();
  }

  /**
   * Chooses an envelope from messageChooser without updating it. This enables flow control
   * on the SSP level, meaning the task will not get further messages for the SSP if it cannot
   * process it. The chooser is updated only after the callback to process is invoked, then the task
   * is able to process more messages. This flow control does not block. so in case of empty message chooser,
   * it will return null immediately without blocking, and the chooser will not poll the underlying system
   * consumer since there are still messages in the SystemConsumers buffer.
   */
  private IncomingMessageEnvelope chooseEnvelope() {
    IncomingMessageEnvelope envelope = consumerMultiplexer.choose(false);
    if (envelope != null) {
      log.trace("Choose envelope ssp {} offset {} for processing",
          envelope.getSystemStreamPartition(elasticityFactor), envelope.getOffset());
      containerMetrics.envelopes().inc();
    } else {
      log.trace("No envelope is available");
      containerMetrics.nullEnvelopes().inc();
    }
    return envelope;
  }

  /**
   * Insert the envelope into the task pending queues and run all the tasks
   */
  private void runTasks(IncomingMessageEnvelope envelope) {
    if (!shutdownNow) {
      if (envelope != null) {
        PendingEnvelope pendingEnvelope = new PendingEnvelope(envelope);
        // when elasticity is enabled
        // the tasks actually consume a keyBucket of the ssp.
        // hence use the SSP with keybucket to find the worker(s) for the envelope
        List<AsyncTaskWorker> listOfWorkersForEnvelope = getWorkersForEnvelope(envelope);
        if (listOfWorkersForEnvelope != null) {
          for (AsyncTaskWorker worker : listOfWorkersForEnvelope) {
            worker.state.insertEnvelope(pendingEnvelope);
          }
        } else if (elasticityFactor > 1) {
          // is listOfWorkersForEnvelope is null and elascity factor > 1 (aka enabled), then
          // this condition happens when a keyBucket of the SSP is being consumed but other keyBuckets are not consumed
          // if this update is not done for the SSP then the unprocessed envelopes from other keyBuckets
          // will make the consumerMultiplexer not poll as it sees envelopes available for consumption.
          consumerMultiplexer.tryUpdate(envelope.getSystemStreamPartition(elasticityFactor));
          log.trace("updating the system consumers for ssp keyBucket {} not processed by this runloop",
              envelope.getSystemStreamPartition(elasticityFactor));
        }
      }

      for (AsyncTaskWorker worker: taskWorkers) {
        worker.run();
      }
    }
  }

  /**
   * when elasticity is not enabled, fetch the workers from sspToTaskWorkerMapping using envelope.getSSP()
   * when elasticity is enabled,
   *       sspToTaskWorkerMapping has workers for a SSP which has keyBucket
   *       hence need to use envelop.getSSP(elasticityFactor)
   *       Additionally, when envelope is EnofStream or Watermark, it needs to be sent to all works for the ssp irrespective of keyBucket
   * @param envelope
   * @return list of workers for the envelope
   */
  private List<AsyncTaskWorker> getWorkersForEnvelope(IncomingMessageEnvelope envelope) {
    if (elasticityFactor <= 1) {
      return sspToTaskWorkerMapping.get(envelope.getSystemStreamPartition());
    }

    final SystemStreamPartition sspOfEnvelope = envelope.getSystemStreamPartition(elasticityFactor);
    List<AsyncTaskWorker> listOfWorkersForEnvelope = null;

    // if envelope is end of stream or watermark, it needs to be routed to all tasks consuming the ssp irresp of keybucket
    MessageType messageType = MessageType.of(envelope.getMessage());
    if (envelope.isEndOfStream() || MessageType.END_OF_STREAM == messageType || MessageType.WATERMARK == messageType) {

      //sspToTaskWorkerMapping has ssps with keybucket so extract and check only system, stream and partition and ignore the keybucket
      listOfWorkersForEnvelope = sspToTaskWorkerMapping.entrySet()
          .stream()
          .filter(sspToTask -> sspToTask.getKey().getSystemStream().equals(sspOfEnvelope.getSystemStream())
              && sspToTask.getKey().getPartition().equals(sspOfEnvelope.getPartition()))
          .map(sspToWorker -> sspToWorker.getValue())
          .flatMap(Collection::stream)
          .collect(Collectors.toList());
    } else {
      listOfWorkersForEnvelope = sspToTaskWorkerMapping.get(sspOfEnvelope);
    }
    return listOfWorkersForEnvelope;
  }

  /**
   * Block the runloop thread if all tasks are busy. When a task worker finishes or window/commit completes,
   * it will resume the runloop.
   *
   * In addition, delay the RunLoop thread for a short time if there are no new messages to process and the run loop
   * has not been resumed since the last time this code was run. This will prevent the main thread from spinning when it
   * has no work to distribute. If a task worker finishes or window/commit completes before the timeout then resume
   * the RunLoop thread immediately. That event may allow a task worker to start processing a message that has already
   * been chosen.  In any event it should only delay for a short time.  It needs to periodically check for new messages.
   */
  private void blockIfBusyOrNoNewWork(IncomingMessageEnvelope envelope) {
    synchronized (latch) {

      // First check to see if we should delay the run loop for a short time.  The runLoopResumedSinceLastChecked boolean
      // is used to ensure we don't delay if there may already be a task ready to dequeue a previously chosen/pending
      // message. It is better to occasionally make one additional loop when there is no work to do then delay the
      // runloop when there is work that could be started immediately.
      if ((envelope == null) && !runLoopResumedSinceLastChecked) {
        try {
          log.trace("Start no work wait");
          latch.wait(maxIdleMs);
          log.trace("End no work wait");
        } catch (InterruptedException e) {
          throw new SamzaException("Run loop is interrupted", e);
        }
      }
      runLoopResumedSinceLastChecked = false;

      // Next check to see if we should block if all the tasks are busy.
      while (!shutdownNow && throwable == null) {
        for (AsyncTaskWorker worker : taskWorkers) {
          if (worker.state.isReady()) {
            return;
          }
        }

        try {
          log.trace("Block loop thread");
          latch.wait();
        } catch (InterruptedException e) {
          throw new SamzaException("Run loop is interrupted", e);
        }
      }
    }
  }

  /**
   * Resume the runloop thread. This API is triggered in the following scenarios:
   * A. A task becomes ready to process a message.
   * B. A task has failed when processing a message.
   * C. User thread shuts down the run loop.
   */
  private void resume() {
    log.trace("Resume loop thread");
    if (coordinatorRequests.shouldShutdownNow() && coordinatorRequests.commitRequests().isEmpty()) {
      shutdownNow = true;
    }
    synchronized (latch) {
      latch.notifyAll();
      runLoopResumedSinceLastChecked = true;
    }
  }

  /**
   * Set the throwable and abort run loop. The throwable will be thrown from the run loop thread
   * @param t throwable
   */
  private void abort(Throwable t) {
    throwable = t;
  }

  /**
   * PendingEnvenlope contains an envelope that is not processed by this task, and
   * a flag indicating whether it has been processed by any tasks.
   */
  private static final class PendingEnvelope {
    private final IncomingMessageEnvelope envelope;
    private boolean processed = false;

    PendingEnvelope(IncomingMessageEnvelope envelope) {
      this.envelope = envelope;
    }

    /**
     * Returns true if the envelope has not been processed.
     */
    private boolean markProcessed() {
      boolean oldValue = processed;
      processed = true;
      return !oldValue;
    }
  }


  private enum WorkerOp {
    WINDOW,
    COMMIT,
    PROCESS,
    END_OF_STREAM,
    SCHEDULER,
    NO_OP
  }

  /**
   * The AsyncTaskWorker encapsulates the states of an {@link org.apache.samza.task.AsyncStreamTask}. If the task becomes ready, it
   * will run the task asynchronously. It runs window and commit in the provided thread pool.
   */
  private class AsyncTaskWorker implements TaskCallbackListener {
    private final RunLoopTask task;
    private final TaskCallbackManager callbackManager;
    private volatile AsyncTaskState state;

    AsyncTaskWorker(RunLoopTask task) {
      this.task = task;
      this.callbackManager = new TaskCallbackManager(this, callbackTimer, callbackTimeoutMs, maxConcurrency, clock);
      Set<SystemStreamPartition> sspSet = getWorkingSSPSet(task);
      this.state = new AsyncTaskState(task.taskName(), task.metrics(), sspSet, !task.intermediateStreams().isEmpty());
    }

    private void init() {
      // schedule the timer for windowing and commiting
      if (task.isWindowableTask() && windowMs > 0L) {
        workerTimer.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            log.trace("Task {} need window", task.taskName());
            state.needWindow();
            resume();
          }
        }, windowMs, windowMs, TimeUnit.MILLISECONDS);
      }

      if (commitMs > 0L) {
        workerTimer.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            log.trace("Task {} need commit", task.taskName());
            state.needCommit();
            resume();
          }
        }, commitMs, commitMs, TimeUnit.MILLISECONDS);
      }

      final EpochTimeScheduler epochTimeScheduler = task.epochTimeScheduler();
      if (epochTimeScheduler != null) {
        epochTimeScheduler.registerListener(() -> {
          state.needScheduler();
        });
      }
    }

    /**
     * Returns those partitions for the task for which we have not received end-of-stream from the consumer.
     * @param task
     * @return a Set of SSPs such that all SSPs are not at end of stream.
     */
    private Set<SystemStreamPartition> getWorkingSSPSet(RunLoopTask task) {

      Set<SystemStreamPartition> allPartitions = task.systemStreamPartitions();

      // filter only those SSPs that are not at end of stream.
      Set<SystemStreamPartition> workingSSPSet = allPartitions.stream()
          .filter(ssp -> !consumerMultiplexer.isEndOfStream(ssp))
          .collect(Collectors.toSet());
      return workingSSPSet;
    }

    /**
     * Invoke next task operation based on its state
     */
    private void run() {
      switch (state.nextOp()) {
        case PROCESS:
          process();
          break;
        case WINDOW:
          window();
          break;
        case SCHEDULER:
          scheduler();
          break;
        case COMMIT:
          commit();
          break;
        case END_OF_STREAM:
          endOfStream();
          break;
        default:
          //no op
          break;
      }
    }

    private void endOfStream() {
      state.complete = true;
      try {
        ReadableCoordinator coordinator = new ReadableCoordinator(task.taskName());

        task.endOfStream(coordinator);
        // issue a request for shutdown of the task
        coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
        coordinatorRequests.update(coordinator);

        // invoke commit on the task - if the endOfStream callback had requested a final commit.
        boolean needFinalCommit = coordinatorRequests.commitRequests().remove(task.taskName());
        if (needFinalCommit) {
          task.commit();
        }
      } finally {
        resume();
      }

    }

    /**
     * Process asynchronously. The callback needs to be fired once the processing is done.
     */
    private void process() {
      final IncomingMessageEnvelope envelope = state.fetchEnvelope();
      log.trace("Process ssp {} offset {}", envelope.getSystemStreamPartition(elasticityFactor), envelope.getOffset());

      final ReadableCoordinator coordinator = new ReadableCoordinator(task.taskName());
      TaskCallbackFactory callbackFactory = new TaskCallbackFactory() {
        @Override
        public TaskCallback createCallback() {
          state.startProcess();
          containerMetrics.processes().inc();
          return callbackManager.createCallback(task.taskName(), envelope, coordinator);
        }
      };

      task.process(envelope, coordinator, callbackFactory);
    }

    /**
     * Invoke window. Run window in thread pool if not the single thread mode.
     */
    private void window() {
      state.startWindow();
      Runnable windowWorker = new Runnable() {
        @Override
        public void run() {
          try {
            containerMetrics.windows().inc();

            ReadableCoordinator coordinator = new ReadableCoordinator(task.taskName());
            long startTime = clock.nanoTime();
            task.window(coordinator);
            containerMetrics.windowNs().update(clock.nanoTime() - startTime);

            /**
             * Window calls that execute for more than task.window.ms will starve process calls
             * since window has higher priority than process in {@link AsyncTaskState#nextOp()}.
             * Warn the users if this is the case.
             */
            long averageWindowMs = TimeUnit.NANOSECONDS.toMillis(
                (long) containerMetrics.windowNs().getSnapshot().getAverage());
            if (averageWindowMs >= windowMs) {
              log.warn("Average window call duration {} is greater than the configured task.window.ms {}. " +
                      "This can starve process calls, so consider setting task.window.ms >> {} ms.",
                  new Object[]{averageWindowMs, windowMs, averageWindowMs});
            }

            coordinatorRequests.update(coordinator);

            state.doneWindow();
          } catch (Throwable t) {
            log.error("Task {} window failed", task.taskName(), t);
            abort(t);
          } finally {
            log.trace("Task {} window completed", task.taskName());
            resume();
          }
        }
      };

      if (threadPool != null) {
        log.trace("Task {} window on the thread pool", task.taskName());
        threadPool.submit(windowWorker);
      } else {
        log.trace("Task {} window on the run loop thread", task.taskName());
        windowWorker.run();
      }
    }

    /**
     * Invoke commit. Run commit in thread pool if not the single thread mode.
     */
    private void commit() {
      state.startCommit();
      Runnable commitWorker = new Runnable() {
        @Override
        public void run() {
          try {
            containerMetrics.commits().inc();

            long startTime = clock.nanoTime();
            task.commit();
            containerMetrics.commitNs().update(clock.nanoTime() - startTime);

            state.doneCommit();
          } catch (Throwable t) {
            log.error("Task {} commit failed", task.taskName(), t);
            abort(t);
          } finally {
            log.trace("Task {} commit completed", task.taskName());
            resume();
          }
        }
      };

      if (threadPool != null) {
        log.trace("Task {} commits on the thread pool", task.taskName());
        threadPool.submit(commitWorker);
      } else {
        log.trace("Task {} commits on the run loop thread", task.taskName());
        commitWorker.run();
      }
    }

    private void scheduler() {
      state.startScheduler();
      Runnable timerWorker = new Runnable() {
        @Override
        public void run() {
          try {
            ReadableCoordinator coordinator = new ReadableCoordinator(task.taskName());

            long startTime = clock.nanoTime();
            task.scheduler(coordinator);
            containerMetrics.timerNs().update(clock.nanoTime() - startTime);

            coordinatorRequests.update(coordinator);
            state.doneScheduler();
          } catch (Throwable t) {
            log.error("Task {} scheduler failed", task.taskName(), t);
            abort(t);
          } finally {
            log.trace("Task {} scheduler completed", task.taskName());
            resume();
          }
        }
      };

      if (threadPool != null) {
        log.trace("Task {} scheduler runs on the thread pool", task.taskName());
        threadPool.submit(timerWorker);
      } else {
        log.trace("Task {} scheduler runs on the run loop thread", task.taskName());
        timerWorker.run();
      }
    }

    /**
     * Task process completes successfully, update the offsets based on the high-water mark.
     * Then it will trigger the listener for task state change.
     * * @param callback AsyncSteamTask.processAsync callback
     */
    @Override
    public void onComplete(final TaskCallback callback) {
      long workNanos = clock.nanoTime() - ((TaskCallbackImpl) callback).getTimeCreatedNs();
      callbackExecutor.schedule(new Runnable() {
        @Override
        public void run() {
          try {
            state.doneProcess();
            state.taskMetrics.asyncCallbackCompleted().inc();
            TaskCallbackImpl callbackImpl = (TaskCallbackImpl) callback;
            containerMetrics.processNs().update(clock.nanoTime() - callbackImpl.getTimeCreatedNs());
            log.trace("Got callback complete for task {}, ssp {}",
                callbackImpl.getTaskName(), callbackImpl.getSystemStreamPartition());

            List<TaskCallbackImpl> callbacksToUpdate = callbackManager.updateCallback(callbackImpl);
            for (TaskCallbackImpl callbackToUpdate : callbacksToUpdate) {
              log.trace("Update offset for ssp {}, offset {}", callbackToUpdate.getSystemStreamPartition(),
                  callbackToUpdate.getOffset());

              // update offset
              if (task.offsetManager() != null) {
                task.offsetManager().update(task.taskName(), callbackToUpdate.getSystemStreamPartition(),
                    callbackToUpdate.getOffset());
              }

              // update coordinator
              coordinatorRequests.update(callbackToUpdate.getCoordinator());
            }
          } catch (Throwable t) {
            log.error("Error marking process as complete.", t);
            abort(t);
          } finally {
            resume();
          }
        }
      }, workNanos);
    }

    /**
     * Task process fails. Trigger the listener indicating failure.
     * @param callback AsyncSteamTask.processAsync callback
     * @param t throwable of the failure
     */
    @Override
    public void onFailure(TaskCallback callback, Throwable t) {
      try {
        // set the exception code ahead of marking the message as processed to make sure the exception
        // is visible to the run loop thread promptly. Refer SAMZA-2510 for more details.
        abort(t);
        state.doneProcess();
        // update pending count, but not offset
        TaskCallbackImpl callbackImpl = (TaskCallbackImpl) callback;
        log.error("Got callback failure for task {}", callbackImpl.getTaskName(), t);
      } catch (Throwable e) {
        log.error("Error marking process as failed.", e);
      } finally {
        resume();
      }
    }
  }


  /**
   * AsyncTaskState manages the state of the AsyncStreamTask. In summary, a worker has the following states:
   * ready - ready for window, commit or process next incoming message.
   * busy - doing window, commit or not able to process next message.
   * idle - no pending messages, and no window/commit
   */
  private final class AsyncTaskState {
    private volatile boolean needWindow = false;
    private volatile boolean needCommit = false;
    private volatile boolean needScheduler = false;
    private volatile boolean complete = false;
    private volatile boolean endOfStream = false;
    private volatile boolean windowInFlight = false;
    private volatile boolean commitInFlight = false;
    private volatile boolean schedulerInFlight = false;
    private final AtomicInteger messagesInFlight = new AtomicInteger(0);
    private final ArrayDeque<PendingEnvelope> pendingEnvelopeQueue;

    //Set of SSPs that we are currently processing for this task instance
    private final Set<SystemStreamPartition> processingSspSet;
    private final TaskName taskName;
    private final TaskInstanceMetrics taskMetrics;
    private final boolean hasIntermediateStreams;

    AsyncTaskState(TaskName taskName, TaskInstanceMetrics taskMetrics, Set<SystemStreamPartition> sspSet, boolean hasIntermediateStreams) {
      this.taskName = taskName;
      this.taskMetrics = taskMetrics;
      this.pendingEnvelopeQueue = new ArrayDeque<>();
      this.processingSspSet = sspSet;
      this.hasIntermediateStreams = hasIntermediateStreams;
    }

    private boolean checkEndOfStream() {
      if (pendingEnvelopeQueue.size() == 1) {
        PendingEnvelope pendingEnvelope = pendingEnvelopeQueue.peek();
        IncomingMessageEnvelope envelope = pendingEnvelope.envelope;

        if (envelope.isEndOfStream()) {
          SystemStreamPartition ssp = envelope.getSystemStreamPartition(elasticityFactor);
          processingSspSet.remove(ssp);
          if (!hasIntermediateStreams) {
            pendingEnvelopeQueue.remove();
          }
        }
      }
      return processingSspSet.isEmpty();
    }

    /**
     * Returns whether the task is ready to do process/window/commit.
     *
     */
    private boolean isReady() {
      if (checkEndOfStream()) {
        endOfStream = true;
      }
      if (coordinatorRequests.commitRequests().remove(taskName)) {
        needCommit = true;
      }

      boolean opInFlight = windowInFlight || commitInFlight || schedulerInFlight;
      /*
       * A task is ready to commit, when task.commit(needCommit) is requested either by user or commit thread
       * and either of the following conditions are true.
       * a) When process, window, commit and scheduler are not in progress.
       * b) When task.async.commit is true and window, commit are not in progress.
       */
      if (needCommit) {
        return (messagesInFlight.get() == 0 || isAsyncCommitEnabled) && !opInFlight;
      } else if (needWindow || needScheduler || endOfStream) {
        /*
         * A task is ready for window, scheduler or end-of-stream operation.
         */
        return messagesInFlight.get() == 0 && !opInFlight;
      } else {
        /*
         * A task is ready to process new message, when number of task.process calls in progress < task.max.concurrency
         * and either of the following conditions are true.
         * a) When window, commit and scheduler are not in progress.
         * b) When task.async.commit is true and window and scheduler are not in progress.
         */
        return messagesInFlight.get() < maxConcurrency && !windowInFlight && !schedulerInFlight && (isAsyncCommitEnabled || !commitInFlight);
      }
    }

    /**
     * Returns the next operation by this taskWorker
     */
    private WorkerOp nextOp() {

      if (complete) return WorkerOp.NO_OP;

      if (isReady()) {
        if (needCommit) return WorkerOp.COMMIT;
        else if (needWindow) return WorkerOp.WINDOW;
        else if (needScheduler) return WorkerOp.SCHEDULER;
        else if (endOfStream && pendingEnvelopeQueue.isEmpty()) return WorkerOp.END_OF_STREAM;
        else if (!pendingEnvelopeQueue.isEmpty()) return WorkerOp.PROCESS;
      }
      return WorkerOp.NO_OP;
    }

    private void needWindow() {
      needWindow = true;
    }

    private void needCommit() {
      needCommit = true;
    }

    private void needScheduler() {
      needScheduler = true;
    }

    private void startWindow() {
      needWindow = false;
      windowInFlight = true;
    }

    private void startCommit() {
      needCommit = false;
      commitInFlight = true;
    }

    private void startProcess() {
      int count = messagesInFlight.incrementAndGet();
      taskMetrics.messagesInFlight().set(count);
    }

    private void startScheduler() {
      needScheduler = false;
      schedulerInFlight = true;
    }

    private void doneCommit() {
      commitInFlight = false;
    }

    private void doneWindow() {
      windowInFlight = false;
    }

    private void doneProcess() {
      int count = messagesInFlight.decrementAndGet();
      taskMetrics.messagesInFlight().set(count);
    }

    private void doneScheduler() {
      schedulerInFlight = false;
    }

    /**
     * Insert an PendingEnvelope into the pending envelope queue.
     * The function will be called in the run loop thread so no synchronization.
     * @param pendingEnvelope
     */
    private void insertEnvelope(PendingEnvelope pendingEnvelope) {
      pendingEnvelopeQueue.add(pendingEnvelope);
      int queueSize = pendingEnvelopeQueue.size();
      taskMetrics.pendingMessages().set(queueSize);
      log.trace("Insert envelope to task {} queue.", taskName);
      log.debug("Task {} pending envelope count is {} after insertion.", taskName, queueSize);
    }


    /**
     * Fetch the pending envelope in the pending queue for the task to process.
     * Update the chooser for flow control on the SSP level. Once it's updated, the RunLoop
     * will be able to choose new messages from this SSP for the task to process. Note that we
     * update only when the envelope is first time being processed. This solves the issue in
     * Broadcast stream where a message need to be processed by multiple tasks. In that case,
     * the envelope will be in the pendingEnvelopeQueue of each task. Only the first fetch updates
     * the chooser with the next envelope in the broadcast stream partition.
     * The function will be called in the run loop thread so no synchronization.
     * @return
     */
    private IncomingMessageEnvelope fetchEnvelope() {
      PendingEnvelope pendingEnvelope = pendingEnvelopeQueue.remove();
      int queueSize = pendingEnvelopeQueue.size();
      taskMetrics.pendingMessages().set(queueSize);
      log.trace("fetch envelope ssp {} offset {} to process.",
          pendingEnvelope.envelope.getSystemStreamPartition(elasticityFactor), pendingEnvelope.envelope.getOffset());
      log.debug("Task {} pending envelopes count is {} after fetching.", taskName, queueSize);

      if (pendingEnvelope.markProcessed()) {
        SystemStreamPartition partition = pendingEnvelope.envelope.getSystemStreamPartition(elasticityFactor);
        consumerMultiplexer.tryUpdate(partition);
        log.debug("Update chooser for {}", partition);
      }
      return pendingEnvelope.envelope;
    }
  }
}
