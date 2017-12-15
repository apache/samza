package org.apache.samza.test.processor;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by yipan on 12/13/17.
 */
public class SharedContextFactories {

  public static class SharedContextFactory {
    static Map<String, SharedContextFactory> sharedContextFactories = new HashMap<>();

    Map<String, Object> sharedObjects = new HashMap<>();

    public Object getSharedObject(String resourceName) {
      return this.sharedObjects.get(resourceName);
    }

    public void addSharedObject(String resourceName, Object sharedObj) {
      this.sharedObjects.putIfAbsent(resourceName, sharedObj);
    }

    public static SharedContextFactory getInstance(String taskName) {
      if (sharedContextFactories.get(taskName) == null) {
        sharedContextFactories.putIfAbsent(taskName, new SharedContextFactory());
      }
      return sharedContextFactories.get(taskName);
    }

    public static void clearAll() {
      sharedContextFactories.clear();
    }
  }

  public static class ProcessorSharedContextFactory extends SharedContextFactory {
    static Map<String, ProcessorSharedContextFactory> processorSharedFactories = new HashMap<>();

    private final String processorId;

    SharedContextFactory getTaskSharedContextFactory(String taskName) {
      String globalTaskName = String.format("%s-%s", this.processorId, taskName);
      return SharedContextFactory.getInstance(globalTaskName);
    }

    public static ProcessorSharedContextFactory getInstance(String processorId) {
      if (processorSharedFactories.get(processorId) == null) {
        processorSharedFactories.putIfAbsent(processorId, new ProcessorSharedContextFactory(processorId));
      }
      return processorSharedFactories.get(processorId);
    }

    ProcessorSharedContextFactory(String processorId) {
      this.processorId = processorId;
    }

    public static void clearAll() {
      processorSharedFactories.clear();
    }
  }

  public static class GlobalSharedContextFactory extends SharedContextFactory {
    static Map<String, GlobalSharedContextFactory> globalSharedContextFactories = new HashMap<>();

    private final String appName;

    GlobalSharedContextFactory(String appName) {
      this.appName = appName;
    }

    ProcessorSharedContextFactory getProcessorSharedContextFactory(String processorName) {
      String globalProcessorName = String.format("%s-%s", this.appName, processorName);
      return ProcessorSharedContextFactory.getInstance(globalProcessorName);
    }

    public static GlobalSharedContextFactory getInstance(String appName) {
      if (globalSharedContextFactories.get(appName) == null) {
        globalSharedContextFactories.putIfAbsent(appName, new GlobalSharedContextFactory(appName));
      }
      return globalSharedContextFactories.get(appName);
    }

    public static void clearAll() {
      globalSharedContextFactories.clear();
    }
  }

  public static GlobalSharedContextFactory getGlobalSharedContextFactory(String appName) {
    return GlobalSharedContextFactory.getInstance(appName);
  }

  public static void clearAll() {
    GlobalSharedContextFactory.clearAll();
    ProcessorSharedContextFactory.clearAll();
    SharedContextFactory.clearAll();
  }
}