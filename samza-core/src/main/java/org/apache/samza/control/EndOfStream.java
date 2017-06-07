package org.apache.samza.control;

import org.apache.samza.system.SystemStream;


public interface EndOfStream {

  boolean isEndOfStream(SystemStream systemStream);

  void updateEndOfStream(SystemStream systemStream);
}
