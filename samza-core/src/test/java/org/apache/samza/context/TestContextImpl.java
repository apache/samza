package org.apache.samza.context;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestContextImpl {
  /**
   * Given a concrete context, getApplicationDefinedContainerContext should return it.
   */
  @Test
  public void testGetApplicationDefinedContainerContext() {
    MockApplicationDefinedContainerContext applicationDefinedContainerContext =
        new MockApplicationDefinedContainerContext();
    Context context = buildWithApplicationDefinedContainerContext(applicationDefinedContainerContext);
    assertEquals(applicationDefinedContainerContext,
        context.getApplicationDefinedContainerContext(MockApplicationDefinedContainerContext.class));
  }

  /**
   * Given no concrete context, getApplicationDefinedContainerContext should throw an exception.
   */
  @Test(expected = IllegalStateException.class)
  public void testGetMissingApplicationDefinedContainerContext() {
    Context context = buildWithApplicationDefinedContainerContext(null);
    context.getApplicationDefinedContainerContext(MockApplicationDefinedContainerContext.class);
  }

  /**
   * Given a concrete context but the incorrect type, getApplicationDefinedContainerContext should throw an exception.
   */
  @Test(expected = ClassCastException.class)
  public void testGetWrongTypeApplicationDefinedContainerContext() {
    ApplicationDefinedContainerContext applicationDefinedContainerContext =
        mock(ApplicationDefinedContainerContext.class);
    Context context = buildWithApplicationDefinedContainerContext(applicationDefinedContainerContext);
    context.getApplicationDefinedContainerContext(MockApplicationDefinedContainerContext.class);
  }

  /**
   * Given a concrete context, getApplicationDefinedTaskContext should return it.
   */
  @Test
  public void testGetApplicationDefinedTaskContext() {
    MockApplicationDefinedTaskContext applicationDefinedTaskContext = new MockApplicationDefinedTaskContext();
    Context context = buildWithApplicationDefinedTaskContext(applicationDefinedTaskContext);
    assertEquals(applicationDefinedTaskContext,
        context.getApplicationDefinedTaskContext(MockApplicationDefinedTaskContext.class));
  }

  /**
   * Given no concrete context, getApplicationDefinedTaskContext should throw an exception.
   */
  @Test(expected = IllegalStateException.class)
  public void testGetMissingApplicationDefinedTaskContext() {
    Context context = buildWithApplicationDefinedTaskContext(null);
    context.getApplicationDefinedTaskContext(MockApplicationDefinedTaskContext.class);
  }

  /**
   * Given a concrete context but the incorrect type, getApplicationDefinedTaskContext should throw an exception.
   */
  @Test(expected = ClassCastException.class)
  public void testGetWrongTypeApplicationDefinedTaskContext() {
    ApplicationDefinedTaskContext applicationDefinedTaskContext = mock(ApplicationDefinedTaskContext.class);
    Context context = buildWithApplicationDefinedTaskContext(applicationDefinedTaskContext);
    context.getApplicationDefinedTaskContext(MockApplicationDefinedTaskContext.class);
  }

  private static Context buildWithApplicationDefinedContainerContext(
      ApplicationDefinedContainerContext applicationDefinedContainerContext) {
    return new ContextImpl(null, null, null, applicationDefinedContainerContext, null);
  }

  private static Context buildWithApplicationDefinedTaskContext(
      ApplicationDefinedTaskContext applicationDefinedTaskContext) {
    return new ContextImpl(null, null, null, null, applicationDefinedTaskContext);
  }

  /**
   * Simple empty implementation for testing.
   */
  private class MockApplicationDefinedContainerContext implements ApplicationDefinedContainerContext {
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
  private class MockApplicationDefinedTaskContext implements ApplicationDefinedTaskContext {
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
  }
}