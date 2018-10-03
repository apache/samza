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
package org.apache.samza.context;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestContextImpl {
  /**
   * Given a concrete context, getApplicationContainerContext should return it.
   */
  @Test
  public void testGetApplicationContainerContext() {
    MockApplicationContainerContext applicationContainerContext = new MockApplicationContainerContext();
    Context context = buildWithApplicationContainerContext(applicationContainerContext);
    assertEquals(applicationContainerContext, context.getApplicationContainerContext());
  }

  /**
   * Given no concrete context, getApplicationContainerContext should throw an exception.
   */
  @Test(expected = IllegalStateException.class)
  public void testGetMissingApplicationContainerContext() {
    Context context = buildWithApplicationContainerContext(null);
    context.getApplicationContainerContext();
  }

  /**
   * Given a concrete context, getApplicationTaskContext should return it.
   */
  @Test
  public void testGetApplicationTaskContext() {
    MockApplicationTaskContext applicationTaskContext = new MockApplicationTaskContext();
    Context context = buildWithApplicationTaskContext(applicationTaskContext);
    assertEquals(applicationTaskContext, context.getApplicationTaskContext());
  }

  /**
   * Given no concrete context, getApplicationTaskContext should throw an exception.
   */
  @Test(expected = IllegalStateException.class)
  public void testGetMissingApplicationTaskContext() {
    Context context = buildWithApplicationTaskContext(null);
    context.getApplicationTaskContext();
  }

  private static Context buildWithApplicationContainerContext(ApplicationContainerContext applicationContainerContext) {
    return new ContextImpl(null, null, null, applicationContainerContext, null);
  }

  private static Context buildWithApplicationTaskContext(ApplicationTaskContext applicationTaskContext) {
    return new ContextImpl(null, null, null, null, applicationTaskContext);
  }

  /**
   * Simple empty implementation for testing.
   */
  private class MockApplicationContainerContext implements ApplicationContainerContext {
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
  }

  /**
   * Simple empty implementation for testing.
   */
  private class MockApplicationTaskContext implements ApplicationTaskContext {
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
  }
}