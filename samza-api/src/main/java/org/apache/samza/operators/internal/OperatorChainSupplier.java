package org.apache.samza.operators.internal;

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.data.Message;
import org.apache.samza.task.TaskContext;

/**
 * Created by jvenkatr on 10/22/16.
 */
public interface OperatorChainSupplier {
  public <M extends Message> OperatorChain<M> create(MessageStream<M> source, TaskContext context);
}
