package org.apache.samza.context;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestContextProvider {
  @Test
  public void testBuild() {
    JobContext jobContext = mock(JobContext.class);
    ContainerContext containerContext = mock(ContainerContext.class);
    SamzaContainerContext samzaContainerContext = new SamzaContainerContextImpl(jobContext, containerContext);
    ApplicationDefinedContainerContext applicationDefinedContainerContext =
        mock(ApplicationDefinedContainerContext.class);
    ContextProvider provider = new ContextProvider(samzaContainerContext, applicationDefinedContainerContext);
    TaskContext taskContext = mock(TaskContext.class);
    ApplicationDefinedTaskContext applicationDefinedTaskContext = mock(ApplicationDefinedTaskContext.class);
    Context expected = new ContextImpl(jobContext, containerContext, taskContext, applicationDefinedContainerContext,
        applicationDefinedTaskContext);
    assertEquals(expected, provider.build(taskContext, applicationDefinedTaskContext));
  }
}