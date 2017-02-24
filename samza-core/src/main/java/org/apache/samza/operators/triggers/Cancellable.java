package org.apache.samza.operators.triggers;

/**
 *
 */
public interface Cancellable {
  public boolean cancel();
}
