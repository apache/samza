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

import java.util.Optional;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;


public class TestContextImpl {
  /**
   * Given a concrete context, getApplicationContainerContext should return it.
   */
  @Test
  public void testGetApplicationContainerContext() {
    ApplicationContainerContext applicationContainerContext = mock(ApplicationContainerContext.class);
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
    ApplicationTaskContext applicationTaskContext = mock(ApplicationTaskContext.class);
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

  /**
   * Given a concrete context, getExternalContext should return it.
   */
  @Test
  public void testGetExternalContext() {
    ExternalContext externalContext = mock(ExternalContext.class);
    Context context = buildWithExternalContext(externalContext);
    assertEquals(externalContext, context.getExternalContext());
  }

  /**
   * Given no concrete context, getExternalContext should throw an exception.
   */
  @Test(expected = IllegalStateException.class)
  public void testGetMissingExternalContext() {
    Context context = buildWithExternalContext(null);
    context.getExternalContext();
  }

  private static Context buildWithApplicationContainerContext(ApplicationContainerContext applicationContainerContext) {
    return buildWithApplicationContext(applicationContainerContext, mock(ApplicationTaskContext.class),
        mock(ExternalContext.class));
  }

  private static Context buildWithApplicationTaskContext(ApplicationTaskContext applicationTaskContext) {
    return buildWithApplicationContext(mock(ApplicationContainerContext.class), applicationTaskContext,
        mock(ExternalContext.class));
  }

  private static Context buildWithExternalContext(ExternalContext externalContext) {
    return buildWithApplicationContext(mock(ApplicationContainerContext.class), mock(ApplicationTaskContext.class),
        externalContext);
  }

  private static Context buildWithApplicationContext(ApplicationContainerContext applicationContainerContext,
      ApplicationTaskContext applicationTaskContext, ExternalContext externalContext) {
    return new ContextImpl(mock(JobContext.class), mock(ContainerContext.class), mock(TaskContext.class),
        Optional.ofNullable(applicationContainerContext), Optional.ofNullable(applicationTaskContext),
        Optional.ofNullable(externalContext));
  }
}