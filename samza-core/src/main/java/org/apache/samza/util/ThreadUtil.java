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
package org.apache.samza.util;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadUtil.class);
  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

  public static void logThreadDump(String message) {
    try {
      ThreadInfo[] threadInfo = THREAD_MX_BEAN.dumpAllThreads(true, true);
      StringBuilder sb = new StringBuilder();
      sb.append(message).append("\n");
      for (ThreadInfo ti: threadInfo) {
        sb.append(toString(ti)).append("\n");
      }
      LOGGER.info(sb.toString());
    } catch (Exception e) {
      LOGGER.error("Could not get and log a thread dump.", e);
    }
  }

  /**
   * Copy of ThreadInfo#toString() without the hardcoded MAX_FRAMES = 8 restriction on thread stack depth.
   *
   * Returns a string representation of this thread info.
   * The format of this string depends on the implementation.
   * The returned string will typically include
   * the thread name, the getThreadId thread ID, its state,
   * and a stack trace if any.
   *
   * @return a string representation of this thread info.
   */
  private static String toString(ThreadInfo info) {
    StringBuilder sb = new StringBuilder("\"" + info.getThreadName() + "\""
        + " Id=" + info.getThreadId() + " " + info.getThreadState());
    if (info.getLockName() != null) {
      sb.append(" on " + info.getLockName());
    }
    if (info.getLockOwnerName() != null) {
      sb.append(" owned by \"" + info.getLockOwnerName() + "\" Id="
          + info.getLockOwnerId());
    }
    if (info.isSuspended()) {
      sb.append(" (suspended)");
    }
    if (info.isInNative()) {
      sb.append(" (in native)");
    }
    sb.append('\n');
    int i = 0;
    for (; i < info.getStackTrace().length; i++) {
      StackTraceElement ste = info.getStackTrace()[i];
      sb.append("\tat " + ste.toString());
      sb.append('\n');
      if (i == 0 && info.getLockInfo() != null) {
        Thread.State ts = info.getThreadState();
        switch (ts) {
          case BLOCKED:
            sb.append("\t-  blocked on " + info.getLockInfo());
            sb.append('\n');
            break;
          case WAITING:
            sb.append("\t-  waiting on " + info.getLockInfo());
            sb.append('\n');
            break;
          case TIMED_WAITING:
            sb.append("\t-  waiting on " + info.getLockInfo());
            sb.append('\n');
            break;
          default:
        }
      }

      for (MonitorInfo mi : info.getLockedMonitors()) {
        if (mi.getLockedStackDepth() == i) {
          sb.append("\t-  locked " + mi);
          sb.append('\n');
        }
      }
    }
    if (i < info.getStackTrace().length) {
      sb.append("\t...");
      sb.append('\n');
    }

    LockInfo[] locks = info.getLockedSynchronizers();
    if (locks.length > 0) {
      sb.append("\n\tNumber of locked synchronizers = " + locks.length);
      sb.append('\n');
      for (LockInfo li : locks) {
        sb.append("\t- " + li);
        sb.append('\n');
      }
    }
    sb.append('\n');
    return sb.toString();
  }
}
