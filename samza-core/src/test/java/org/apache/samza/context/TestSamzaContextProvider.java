package org.apache.samza.context;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestSamzaContextProvider {
  @Test
  public void testBuild() {
    JobContext jobContext = mock(JobContext.class);
    ContainerContext containerContext = mock(ContainerContext.class);
    SamzaContainerContext samzaContainerContext = new SamzaContainerContextImpl(jobContext, containerContext);
    SamzaContextProvider provider = new SamzaContextProvider(samzaContainerContext);
    TaskContext taskContext = mock(TaskContext.class);
    SamzaContext expected = new SamzaContextImpl(jobContext, containerContext, taskContext);
    assertEquals(expected, provider.build(taskContext));
  }
}