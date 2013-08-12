package org.apache.samza.test.integration;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

public class StatePerfTestTask implements StreamTask, InitableTask {
  
  private KeyValueStore<String, String> store;
  private int count = 0;
  private int LOG_INTERVAL = 100000;
  private long start = System.currentTimeMillis();
  
  public void init(Config config, TaskContext context) {
    this.store = (KeyValueStore<String, String>) context.getStore("mystore");
  }
  
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    store.put((String) envelope.getMessage(), (String) envelope.getMessage());
    count++;
    if(count % LOG_INTERVAL == 0) {
      double ellapsedSecs = (System.currentTimeMillis() - start)/1000.0;
      System.out.println(String.format("Throughput = %.2f messages/sec.", count/ellapsedSecs));
      start = System.currentTimeMillis();
      count = 0;
      coordinator.commit();
    }
  }

}
