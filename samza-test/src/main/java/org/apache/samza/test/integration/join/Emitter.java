package org.apache.samza.test.integration.join;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

/**
 * This job takes input from "epoch" and for each epoch emits "max" records of form
 *   (key = counter, value = epoch-partition)
 *   
 */
@SuppressWarnings("unchecked")
public class Emitter implements StreamTask, InitableTask, WindowableTask {
  
  private static String EPOCH = "the-epoch";
  private static String COUNT = "the-count";
  
  private KeyValueStore<String, String> state;
  private int max;
  private String partition;

  @Override
  public void init(Config config, TaskContext context) {
    this.state = (KeyValueStore<String, String>) context.getStore("emitter-state");
    this.partition = Integer.toString(context.getPartition().getPartitionId());
    this.max = config.getInt("count");
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    if(envelope.getSystemStreamPartition().getStream().equals("epoch")) {
      int newEpoch = Integer.parseInt((String) envelope.getMessage());
      Integer epoch = getInt(EPOCH);
      if(epoch == null || newEpoch == epoch)
        return;
      if(newEpoch < epoch)
        throw new IllegalArgumentException("Got new epoch " + newEpoch + " which is less than current epoch " + epoch);
      
      // it's a new era, reset current epoch and count
      this.state.put(EPOCH, Integer.toString(epoch));
      this.state.put(COUNT, "0");
      coordinator.commit();
    }
  }
  
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
    Integer epoch = getInt(EPOCH);
    if(epoch == null) {
      resetEpoch();
      return;
    }
    int counter = getInt(COUNT);
    if(counter < max) {
      OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(new SystemStream("kafka", "emitted"), Integer.toString(counter), epoch + "-" + partition);
      collector.send(envelope);
      this.state.put(COUNT, Integer.toString(getInt(COUNT) + 1));
    } else {
      trySleep(100);
    }
  }
  
  private void resetEpoch() {
    state.put(EPOCH, "0");
    state.put(COUNT, "0");
  }
  
  private Integer getInt(String key) {
    String value = this.state.get(key);
    return value == null? null : Integer.parseInt(value);
  }
  
  private void trySleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

}
