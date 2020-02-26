package org.apache.samza.lineage.mock;

import org.apache.samza.config.Config;
import org.apache.samza.lineage.LineageFactory;
import org.apache.samza.lineage.mock.MockLineage;


public class MockLineageFactory implements LineageFactory<MockLineage> {
  @Override
  public MockLineage getLineage(Config config) {
    return new MockLineage();
  }
}
