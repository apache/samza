package org.apache.samza.test.integration.join;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class EpochPartitioner implements Partitioner {
  
  public EpochPartitioner(VerifiableProperties p){}
  
  public int partition(Object key, int numParts) {
    return Integer.parseInt((String) key);
  }
}
