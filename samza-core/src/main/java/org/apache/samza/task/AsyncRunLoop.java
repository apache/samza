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

package org.apache.samza.task;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstance;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.util.HighResolutionClock;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Throttleable;
import org.apache.samza.util.ThrottlingScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


/**
 * The AsyncRunLoop supports multithreading execution of Samza {@link AsyncStreamTask}s.
 */
public class AsyncRunLoop implements Runnable, Throttleable {
  private static final Logger log = LoggerFactory.getLogger(AsyncRunLoop.class);

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
  private final SamzaContainerMetrics containerMetrics;
  private final ScheduledExecutorService workerTimer;
  private final ScheduledExecutorService callbackTimer;
  private final ThrottlingScheduler callbackExecutor;
  private volatile boolean shutdownNow = false;
  private volatile Throwable throwable = null;
  private final HighResolutionClock clock;
  private final boolean isAsyncCommitEnabled;

  public AsyncRunLoop(Map<TaskName, TaskInstance> taskInstances,
      ExecutorService threadPool,
      SystemConsumers consumerMultiplexer,
      int maxConcurrency,
      long windowMs,
      long commitMs,
      long callbackTimeoutMs,
      long maxThrottlingDelayMs,
      SamzaContainerMetrics containerMetrics,
      HighResolutionClock clock,
      boolean isAsyncCommitEnabled) {

    this.threadPool = threadPool;
    this.consumerMultiplexer = consumerMultiplexer;
    this.containerMetrics = containerMetrics;
    this.windowMs = windowMs;
    this.commitMs = commitMs;
    this.maxConcurrency = maxConcurrency;
    this.callbackTimeoutMs = callbackTimeoutMs;
    this.callbackTimer = (callbackTimeoutMs > 0) ? Executors.newSingleThreadScheduledExecutor() : null;
    this.callbackExecutor = new ThrottlingScheduler(maxThrottlingDelayMs);
    this.coordinatorRequests = new CoordinatorRequests(taskInstances.keySet());
    this.latch = new Object();
    this.workerTimer = Executors.newSingleThreadScheduledExecutor();
    this.clock = clock;
    Map<TaskName, AsyncTaskWorker> workers = new HashMap<>();
    for (TaskInstance task : taskInstances.values()) {
      workers.put(task.taskName(), new AsyncTaskWorker(task));
    }
    // Partions and tasks assigned to the container will not change during the run loop life time
    this.sspToTaskWorkerMapping = Collections.unmodifiableMap(getSspToAsyncTaskWorkerMap(taskInstances, workers));
    this.taskWorkers = Collections.unmodifiableList(new ArrayList<>(workers.values()));
    this.isAsyncCommitEnabled = isAsyncCommitEnabled;
  }

  /**
   * Returns mapping of the SystemStreamPartition to the AsyncTaskWorkers to efficiently route the envelopes
   */
  private static Map<SystemStreamPartition, List<AsyncTaskWorker>> getSspToAsyncTaskWorkerMap(
      Map<TaskName, TaskInstance> taskInstances, Map<TaskName, AsyncTaskWorker> taskWorkers) {
    Map<SystemStreamPartition, List<AsyncTaskWorker>> sspToWorkerMap = new HashMap<>();
    for (TaskInstance task : taskInstances.values()) {
      Set<SystemStreamPartition> ssps = JavaConverters.setAsJavaSetConverter(task.systemStreamPartitions()).asJava();
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

      while (!shutdownNow) {
        if (throwable != null) {
          log.error("Caught throwable and stopping run loop", throwable);
          throw new SamzaException(throwable);
        }

        long startNs = clock.nanoTime();

        IncomingMessageEnvelope envelope = chooseEnvelope();
        long chooseNs = clock.nanoTime();

        containerMetrics.chooseNs().update(chooseNs - startNs);

        runTasks(envelope);

        long blockNs = clock.nanoTime();

        blockIfBusy(envelope);

        long currentNs = clock.nanoTime();
        long activeNs = blockNs - chooseNs;
        long totalNs = currentNs - prevNs;
        prevNs = currentNs;
        containerMetrics.blockNs().update(currentNs - blockNs);

        if (totalNs != 0) {
          // totalNs is not 0 if timer metrics are enabled
          containerMetrics.utilization().set(((double) activeNs) / totalNs);
        }
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
          envelope.getSystemStreamPartition(), envelope.getOffset());
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
    if (envelope != null) {
      PendingEnvelope pendingEnvelope = new PendingEnvelope(envelope);
      for (AsyncTaskWorker worker : sspToTaskWorkerMapping.get(envelope.getSystemStreamPartition())) {
        worker.state.insertEnvelope(pendingEnvelope);
      }
    }

    for (AsyncTaskWorker worker: taskWorkers) {
      worker.run();
    }
  }


  /**
   * Block the runloop thread if all tasks are busy. When a task worker finishes or window/commit completes,
   * it will resume the runloop.
   */
  private void blockIfBusy(IncomingMessageEnvelope envelope) {
    synchronized (latch) {
      while (!shutdownNow && throwable == null) {
        for (AsyncTaskWorker worker : taskWorkers) {
          if (worker.state.isReady()) {
            // should continue running if any worker state is ready
            // consumerMultiplexer will block on polling for empty partitions so it won't cause busy loop
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
   * Resume the runloop thread. It is triggered once a task becomes ready again or has failure.
   */
  private void resume() {
    log.trace("Resume loop thread");
    if (coordinatorRequests.shouldShutdownNow() && coordinatorRequests.commitRequests().isEmpty()) {
      shutdownNow = true;
    }
    synchronized (latch) {
      latch.notifyAll();
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
    TIMER,
    NO_OP
  }

  /**
   * The AsyncTaskWorker encapsulates the states of an {@link AsyncStreamTask}. If the task becomes ready, it
   * will run the task asynchronously. It runs window and commit in the provided thread pool.
   */
  private class AsyncTaskWorker implements TaskCallbackListener {
    private final TaskInstance task;
    private final TaskCallbackManager callbackManager;
    private volatile AsyncTaskState state;

    AsyncTaskWorker(TaskInstance task) {
      this.task = task;
      this.callbackManager = new TaskCallbackManager(this, callbackTimer, callbackTimeoutMs, maxConcurrency, clock);
      Set<SystemStreamPartition> sspSet = getWorkingSSPSet(task);
      this.state = new AsyncTaskState(task.taskName(), task.metrics(), sspSet, task.hasIntermediateStreams());
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

      final SystemTimerSchedulerFactory timerFactory = task.context().getTimerFactory();
      if (timerFactory != null) {
        timerFactory.registerListener(() -> {
          state.needTimer();
        });
      }
    }

    /**
     * Returns those partitions for the task for which we have not received end-of-stream from the consumer.
     * @param task
     * @return a Set of SSPs such that all SSPs are not at end of stream.
     */
    private Set<SystemStreamPartition> getWorkingSSPSet(TaskInstance task) {

      Set<SystemStreamPartition> allPartitions = new HashSet<>(JavaConverters.setAsJavaSetConverter(task.systemStreamPartitions()).asJava());

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
        case TIMER:
          timer();
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
      log.trace("Process ssp {} offset {}", envelope.getSystemStreamPartition(), envelope.getOffset());

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

            // A window() that executes for more than task.window.ms, will starve the next process() call
            // when the application has job.thread.pool.size > 1. This is due to prioritizing window() ahead of process()
            // to guarantee window() will fire close to its trigger interval time.
            // We warn the users if the average window execution time is greater than equals to window trigger interval.
            long lowerBoundForWindowTriggerTimeInMs = TimeUnit.NANOSECONDS
                .toMillis((long) containerMetrics.windowNs().getSnapshot().getAverage());
            if (windowMs <= lowerBoundForWindowTriggerTimeInMs) {
              log.warn(
                  "window() call might potentially starve process calls."
                      + " Consider setting task.window.ms > {} ms",
                  lowerBoundForWindowTriggerTimeInMs);
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

    private void timer() {
      state.startTimer();
      Runnable timerWorker = new Runnable() {
        @Override
        public void run() {
          try {
            ReadableCoordinator coordinator = new ReadableCoordinator(task.taskName());

            long startTime = clock.nanoTime();
            task.timer(coordinator);
            containerMetrics.timerNs().update(clock.nanoTime() - startTime);

            coordinatorRequests.update(coordinator);
            state.doneTimer();
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
        threadPool.submit(timerWorker);
      } else {
        log.trace("Task {} commits on the run loop thread", task.taskName());
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
      long workNanos = clock.nanoTime() - ((TaskCallbackImpl) callback).timeCreatedNs;
      callbackExecutor.schedule(new Runnable() {
        @Override
        public void run() {
          try {
            state.doneProcess();
            state.taskMetrics.asyncCallbackCompleted().inc();
            TaskCallbackImpl callbackImpl = (TaskCallbackImpl) callback;
            containerMetrics.processNs().update(clock.nanoTime() - callbackImpl.timeCreatedNs);
            log.trace("Got callback complete for task {}, ssp {}",
                callbackImpl.taskName, callbackImpl.envelope.getSystemStreamPartition());

            List<TaskCallbackImpl> callbacksToUpdate = callbackManager.updateCallback(callbackImpl);
            for (TaskCallbackImpl callbackToUpdate : callbacksToUpdate) {
              IncomingMessageEnvelope envelope = callbackToUpdate.envelope;
              log.trace("Update offset for ssp {}, offset {}", envelope.getSystemStreamPartition(), envelope.getOffset());

              // update offset
              task.offsetManager().update(task.taskName(), envelope.getSystemStreamPartition(), envelope.getOffset());

              // update coordinator
              coordinatorRequests.update(callbackToUpdate.coordinator);
            }
          } catch (Throwable t) {
            log.error(t.getMessage(), t);
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
        state.doneProcess();
        abort(t);
        // update pending count, but not offset
        TaskCallbackImpl callbackImpl = (TaskCallbackImpl) callback;
        log.error("Got callback failure for task {}", callbackImpl.taskName);
      } catch (Throwable e) {
        log.error(e.getMessage(), e);
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
    private volatile boolean needTimer = false;
    private volatile boolean complete = false;
    private volatile boolean endOfStream = false;
    private volatile boolean windowInFlight = false;
    private volatile boolean commitInFlight = false;
    private volatile boolean timerInFlight = false;
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
          SystemStreamPartition ssp = envelope.getSystemStreamPartition();
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

      boolean opInFlight = windowInFlight || commitInFlight || timerInFlight;
      /*
       * A task is ready to commit, when task.commit(needCommit) is requested either by user or commit thread
       * and either of the following conditions are true.
       * a) When process, window, commit and timer are not in progress.
       * b) When task.async.commit is true and window, commit are not in progress.
       */
      if (needCommit) {
        return (messagesInFlight.get() == 0 || isAsyncCommitEnabled) && !opInFlight;
      } else if (needWindow || needTimer || endOfStream) {
        /*
         * A task is ready for window/timer operation, when task.window(needWindow) is requested by either user
         * or window/timer thread. No window, timer, and commit are in progress.
         */
        return messagesInFlight.get() == 0 && !opInFlight;
      } else {
        /*
         * A task is ready to process new message, when number of task.process calls in progress < task.max.concurrency
         * and either of the following conditions are true.
         * a) When window, commit and timer are not in progress.
         * b) When task.async.commit is true and window/timer is not in progress.
         */
        return messagesInFlight.get() < maxConcurrency && !windowInFlight && !timerInFlight && (isAsyncCommitEnabled || !commitInFlight);
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
        else if (needTimer) return WorkerOp.TIMER;
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

    private void needTimer() {
      needTimer = true;
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

    private void startTimer() {
      needTimer = false;
      timerInFlight = true;
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

    private void doneTimer() {
      timerInFlight = false;
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
     * Update the chooser for flow control on the SSP level. Once it's updated, the AsyncRunLoop
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
          pendingEnvelope.envelope.getSystemStreamPartition(), pendingEnvelope.envelope.getOffset());
      log.debug("Task {} pending envelopes count is {} after fetching.", taskName, queueSize);

      if (pendingEnvelope.markProcessed()) {
        SystemStreamPartition partition = pendingEnvelope.envelope.getSystemStreamPartition();
        consumerMultiplexer.tryUpdate(partition);
        log.debug("Update chooser for {}", partition);
      }
      return pendingEnvelope.envelope;
    }
  }
}
