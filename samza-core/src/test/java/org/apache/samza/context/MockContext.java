package org.apache.samza.context;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;

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

  public MockContext() {
    this(new MapConfig());
  }

  /**
   * @param config config is widely used, so help wire it in here
   */
  public MockContext(Config config) {
    when(this.jobContext.getConfig()).thenReturn(config);
  }

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
