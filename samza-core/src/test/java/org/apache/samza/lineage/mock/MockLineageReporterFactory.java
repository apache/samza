package org.apache.samza.lineage.mock;

import org.apache.samza.config.Config;
import org.apache.samza.lineage.LineageReporter;
import org.apache.samza.lineage.LineageReporterFactory;


public class MockLineageReporterFactory implements LineageReporterFactory<MockLineage> {
  @Override
  public LineageReporter<MockLineage> getLineageReporter(Config config) {
    return new MockLineageReporter();
  }
}
