package org.apache.samza.clustermanager;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.loaders.PropertiesConfigLoaderFactory;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.util.ConfigUtil;
import org.apache.samza.util.Util;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.mock;


@RunWith(PowerMockRunner.class)
@PrepareForTest({
    System.class,
    ClusterBasedJobCoordinatorRunner.class,
    ApplicationUtil.class,
    JobCoordinatorLaunchUtil.class
})
public class TestClusterBasedJobCoordinatorRunner {

  @Test
  public void testRunClusterBasedJobCoordinator() throws Exception  {
    Config submissionConfig = new MapConfig(ImmutableMap.of(
        JobConfig.CONFIG_LOADER_FACTORY,
        PropertiesConfigLoaderFactory.class.getName(),
        PropertiesConfigLoaderFactory.CONFIG_LOADER_PROPERTIES_PREFIX + "path",
        getClass().getResource("/test.properties").getPath()));
    Config fullConfig = ConfigUtil.loadConfig(submissionConfig);
    StreamApplication mockApplication = mock(StreamApplication.class);
    PowerMockito.mockStatic(System.class, ApplicationUtil.class, JobCoordinatorLaunchUtil.class);
    PowerMockito
        .when(System.getenv(eq(ShellCommandConfig.ENV_SUBMISSION_CONFIG)))
        .thenReturn(SamzaObjectMapper.getObjectMapper().writeValueAsString(submissionConfig));
    PowerMockito
        .when(ApplicationUtil.fromConfig(any()))
        .thenReturn(mockApplication);
    PowerMockito.doNothing().when(JobCoordinatorLaunchUtil.class, "run", mockApplication, fullConfig);

    ClusterBasedJobCoordinatorRunner.runClusterBasedJobCoordinator(null);

    PowerMockito.verifyStatic(times(1));
    JobCoordinatorLaunchUtil.run(mockApplication, fullConfig);
  }
}
