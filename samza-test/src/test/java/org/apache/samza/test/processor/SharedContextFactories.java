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
package org.apache.samza.test.processor;

import java.util.HashMap;
import java.util.Map;


/**
 * Shared context factories used in unit tests. This is a temporarily solution to enable sharing of test latches in different
 * scope of context (i.e. in the container or the task). This is not intended for production usage.
 */
public class SharedContextFactories {

  public static class SharedContextFactory {
    static Map<String, SharedContextFactory> sharedContextFactories = new HashMap<>();

    Map<String, Object> sharedObjects = new HashMap<>();

    public Object getSharedObject(String resourceName) {
      return this.sharedObjects.get(resourceName);
    }

    public void addSharedObject(String resourceName, Object sharedObj) {
      this.sharedObjects.put(resourceName, sharedObj);
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