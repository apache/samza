package org.apache.samza.operators.impl;

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.internal.OperatorChain;
import org.apache.samza.operators.internal.OperatorChainSupplier;
import org.apache.samza.task.TaskContext;

/**
 * Created by jvenkatr on 10/22/16.
 */
public class ChainedOperatorsFactory implements OperatorChainSupplier {
  /**
   * Static method to create a {@link ChainedOperators} from the {@code source} stream
   *
   * @param source  the input source {@link MessageStream}
   * @param context  the {@link TaskContext} object used to initialize the {@link StateStoreImpl}
   * @return a {@link ChainedOperators} object takes the {@code source} as input
   */
  public <M extends Message> OperatorChain<M> create(MessageStream<M> source, TaskContext context) {
    return new ChainedOperators<>(source, context);
  }

}
