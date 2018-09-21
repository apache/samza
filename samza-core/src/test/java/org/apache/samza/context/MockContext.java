package org.apache.samza.context;

import static org.mockito.Mockito.*;


public class MockContext implements Context {
  private final JobContext jobContext = mock(JobContext.class);
  private final ContainerContext containerContext = mock(ContainerContext.class);
  /**
   * This is {@link TaskContextImpl} because some tests need more than just the interface.
   */
  private final TaskContextImpl taskContext = mock(TaskContextImpl.class);
  private final ApplicationContainerContext applicationContainerContext = mock(ApplicationContainerContext.class);
  private final ApplicationTaskContext applicationTaskContext = mock(ApplicationTaskContext.class);

  @Override
  public JobContext getJobContext() {
    return jobContext;
  }

  @Override
  public ContainerContext getContainerContext() {
    return containerContext;
  }

  @Override
  public TaskContext getTaskContext() {
    return taskContext;
  }

  @Override
  public ApplicationContainerContext getApplicationContainerContext() {
    return applicationContainerContext;
  }

  @Override
  public ApplicationTaskContext getApplicationTaskContext() {
    return applicationTaskContext;
  }
}
