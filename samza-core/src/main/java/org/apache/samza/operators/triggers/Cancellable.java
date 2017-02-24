package org.apache.samza.operators.triggers;

/**
 * Created by jvenkatr on 2/23/17.
 */
public interface Cancellable {
  public boolean cancel();
}
