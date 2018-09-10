package org.apache.samza.context;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestSamzaContainerContextProvider {
  @Test
  public void testBuild() {
    JobContext jobContext = mock(JobContext.class);
    SamzaContainerContextProvider provider = new SamzaContainerContextProvider(jobContext);
    ContainerContext containerContext = mock(ContainerContext.class);
    SamzaContainerContext expected = new SamzaContainerContextImpl(jobContext, containerContext);
    assertEquals(expected, provider.build(containerContext));
  }
}