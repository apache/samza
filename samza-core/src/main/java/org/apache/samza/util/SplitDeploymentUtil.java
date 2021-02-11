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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ShellCommandConfig;


public final class SplitDeploymentUtil {

  /**
   * The split deployment feature uses system env {@code ShellCommandConfig.ENV_SPLIT_DEPLOYMENT_ENABLED} to represent
   * if the user chooses to enable it.
   * This function helps to detect if the split deployment feature is enabled.
   *
   * @return true if split deployment is enabled; vice versa
   */
  public static boolean isSplitDeploymentEnabled() {
    return Boolean.parseBoolean(System.getenv(ShellCommandConfig.ENV_SPLIT_DEPLOYMENT_ENABLED));
  }

  /**
   * Execute the runner class using a separate isolated classloader.
   * @param classLoader {@link ClassLoader} to use to load the runner class which will run
   * @param originalRunnerClass {@link Class} for which will be executed with the new class loader.
   * @param runMethodName run method name of runner class
   * @param runMethodArgs arguments to pass to run method
   */
  public static void runWithClassLoader(ClassLoader classLoader, Class<?> originalRunnerClass, String runMethodName,
      String[] runMethodArgs) {
    // need to use the isolated classloader to load run method and then execute using that new class
    Class<?> runnerClass;
    try {
      runnerClass = classLoader.loadClass(originalRunnerClass.getName());
    } catch (ClassNotFoundException e) {
      throw new SamzaException(String.format(
          "Isolation was enabled, but unable to find %s in isolated classloader", originalRunnerClass.getName()), e);
    }

    // save the current context classloader so it can be reset after finishing the call to run method
    ClassLoader previousContextClassLoader = Thread.currentThread().getContextClassLoader();
    // this is needed because certain libraries (e.g. log4j) use the context classloader
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      executeRunForRunnerClass(runnerClass, runMethodName, runMethodArgs);
    } finally {
      // reset the context class loader; it's good practice, and could be important when running a test suite
      Thread.currentThread().setContextClassLoader(previousContextClassLoader);
    }
  }

  private static void executeRunForRunnerClass(Class<?> runnerClass, String runMethodName, String[] runMethodArgs) {
    Method runMethod;
    try {
      runMethod = runnerClass.getDeclaredMethod(runMethodName, String[].class);
    } catch (NoSuchMethodException e) {
      throw new SamzaException(String.format("Isolation was enabled, but unable to find %s method", runMethodName), e);
    }
    // only sets accessible flag for this method instance
    runMethod.setAccessible(true);

    try {
      // wrapping args in object array so that args is passed as a single argument to the method
      runMethod.invoke(null, new Object[]{runMethodArgs});
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new SamzaException(String.format("Exception while executing %s method", runMethodName), e);
    }
  }
}
