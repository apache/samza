package org.apache.samza.runtime;

import org.apache.samza.config.Config;


public class MockProcessorIdGenerator implements ProcessorIdGenerator {
  @Override
  public String generateProcessorId(Config config) {
    return "testProcessorId";
  }
}