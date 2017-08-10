package org.apache.samza.operators.impl;

import java.util.HashSet;
import java.util.Set;


class EndOfStreamState {
  static final String END_OF_STREAM_STATES = "EndOfStreamStates";

  // set of upstream tasks
  private final Set<String> tasks = new HashSet<>();
  private int expectedTotal = Integer.MAX_VALUE;
  private volatile boolean isEndOfStream = false;

  synchronized void update(String taskName, int taskCount) {
    if (taskName != null) {
      tasks.add(taskName);
    }
    expectedTotal = taskCount;
    isEndOfStream = tasks.size() == expectedTotal;
  }

  boolean isEndOfStream() {
    return isEndOfStream;
  }
}