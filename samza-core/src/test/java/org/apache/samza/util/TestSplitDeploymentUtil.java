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

import org.apache.samza.clustermanager.ClusterBasedJobCoordinator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.AdditionalMatchers.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;


@RunWith(PowerMockRunner.class)
@PrepareForTest({ClusterBasedJobCoordinator.class})
public class TestSplitDeploymentUtil {

  @Test
  public void testRunWithIsolatingClassLoader() throws Exception {
    // partially mock ClusterBasedJobCoordinator (mock runClusterBasedJobCoordinator method only)
    PowerMockito.spy(ClusterBasedJobCoordinator.class);
    // save the context classloader to make sure that it gets set properly once the test is finished
    ClassLoader previousContextClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader classLoader = mock(ClassLoader.class);
    String[] args = new String[]{"arg0", "arg1"};
    doReturn(ClusterBasedJobCoordinator.class).when(classLoader).loadClass(ClusterBasedJobCoordinator.class.getName());

    // stub the private static method which is called by reflection
    PowerMockito.doAnswer(invocation -> {
        // make sure the only calls to this method has the expected arguments
        assertArrayEquals(args, invocation.getArgumentAt(0, String[].class));
        // checks that the context classloader is set correctly
        assertEquals(classLoader, Thread.currentThread().getContextClassLoader());
        return null;
      }).when(ClusterBasedJobCoordinator.class, "runClusterBasedJobCoordinator", any());

    try {
      SplitDeploymentUtil.runWithClassLoader(classLoader,
          ClusterBasedJobCoordinator.class, "runClusterBasedJobCoordinator", args);
      assertEquals(previousContextClassLoader, Thread.currentThread().getContextClassLoader());
    } finally {
      // reset it explicitly just in case runWithClassLoader throws an exception
      Thread.currentThread().setContextClassLoader(previousContextClassLoader);
    }
    // make sure that the classloader got used
    verify(classLoader).loadClass(ClusterBasedJobCoordinator.class.getName());
    // make sure runClusterBasedJobCoordinator only got called once
    verifyPrivate(ClusterBasedJobCoordinator.class).invoke("runClusterBasedJobCoordinator", new Object[]{aryEq(args)});
  }
}
