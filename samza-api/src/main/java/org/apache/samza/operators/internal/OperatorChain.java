package org.apache.samza.operators.internal;

import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

/**
 * Created by jvenkatr on 10/22/16.
 */
public interface OperatorChain<M> {
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator);
  public void onTimer(MessageCollector collector, TaskCoordinator coordinator);
}
