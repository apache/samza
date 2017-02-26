package org.apache.samza.operators.triggers;

/**
 * Represents a task or an operation whose execution can be cancelled.
 */
public interface Cancellable {

  /**
   * Cancel the execution of this operation (if it is not scheduled for execution yet). If the operation is in progress,
   * it is not interrupted / cancelled.
   *
   * @return the result of the cancelation
   */
  public boolean cancel();
}
