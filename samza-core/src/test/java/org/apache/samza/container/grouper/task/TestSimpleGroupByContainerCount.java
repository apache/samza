package org.apache.samza.container.grouper.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.junit.Before;
import org.junit.Test;

import static org.apache.samza.container.mock.ContainerMocks.generateTaskModels;
import static org.apache.samza.container.mock.ContainerMocks.getTaskModel;
import static org.apache.samza.container.mock.ContainerMocks.getTaskName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestSimpleGroupByContainerCount {
  private TaskAssignmentManager taskAssignmentManager;
  private LocalityManager localityManager;

  @Before
  public void setup() {
    taskAssignmentManager = mock(TaskAssignmentManager.class);
    localityManager = mock(LocalityManager.class);
    when(localityManager.getTaskAssignmentManager()).thenReturn(taskAssignmentManager);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupEmptyTasks() {
    new SimpleGroupByContainerCount(1).group(new HashSet());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupFewerTasksThanContainers() {
    Set<TaskModel> taskModels = new HashSet<>();
    taskModels.add(getTaskModel(1));
    new SimpleGroupByContainerCount(2).group(taskModels);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGrouperResultImmutable() {
    Set<TaskModel> taskModels = generateTaskModels(3);
    Set<ContainerModel> containers = new SimpleGroupByContainerCount(3).group(taskModels);
    containers.remove(containers.iterator().next());
  }

  @Test
  public void testGroupHappyPath() {
    Set<TaskModel> taskModels = generateTaskModels(5);

    Set<ContainerModel> containers = new SimpleGroupByContainerCount(2).group(taskModels);

    Map<Integer, ContainerModel> containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getContainerId(), container);
    }

    assertEquals(2, containers.size());
    ContainerModel container0 = containersMap.get(0);
    ContainerModel container1 = containersMap.get(1);
    assertNotNull(container0);
    assertNotNull(container1);
    assertEquals(0, container0.getContainerId());
    assertEquals(1, container1.getContainerId());
    assertEquals(3, container0.getTasks().size());
    assertEquals(2, container1.getTasks().size());
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(3)));
  }

  @Test
  public void testGroupHappyPathWithListOfContainers() {
    Set<TaskModel> taskModels = generateTaskModels(5);

    List<Integer> containerIds = new ArrayList<Integer>() {{
      add(4); add(2);
    }};

    Set<ContainerModel> containers = new SimpleGroupByContainerCount().group(taskModels, containerIds);

    Map<Integer, ContainerModel> containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getContainerId(), container);
    }

    assertEquals(2, containers.size());
    ContainerModel container0 = containersMap.get(4);
    ContainerModel container1 = containersMap.get(2);
    assertNotNull(container0);
    assertNotNull(container1);
    assertEquals(4, container0.getContainerId());
    assertEquals(2, container1.getContainerId());
    assertEquals(3, container0.getTasks().size());
    assertEquals(2, container1.getTasks().size());
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container0.getTasks().containsKey(getTaskName(4)));
    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(3)));
  }


  @Test
  public void testGroupManyTasks() {
    Set<TaskModel> taskModels = generateTaskModels(21);

    List<Integer> containerIds = new ArrayList<Integer>() {{
      add(4); add(2);
    }};


    Set<ContainerModel> containers = new SimpleGroupByContainerCount().group(taskModels, containerIds);

    Map<Integer, ContainerModel> containersMap = new HashMap<>();
    for (ContainerModel container : containers) {
      containersMap.put(container.getContainerId(), container);
    }

    assertEquals(2, containers.size());
    ContainerModel container0 = containersMap.get(4);
    ContainerModel container1 = containersMap.get(2);
    assertNotNull(container0);
    assertNotNull(container1);
    assertEquals(4, container0.getContainerId());
    assertEquals(2, container1.getContainerId());
    assertEquals(11, container0.getTasks().size());
    assertEquals(10, container1.getTasks().size());

    // NOTE: tasks are sorted lexicographically, so the container assignment
    // can seem odd, but the consistency is the key focus
    assertTrue(container0.getTasks().containsKey(getTaskName(0)));
    assertTrue(container0.getTasks().containsKey(getTaskName(10)));
    assertTrue(container0.getTasks().containsKey(getTaskName(12)));
    assertTrue(container0.getTasks().containsKey(getTaskName(14)));
    assertTrue(container0.getTasks().containsKey(getTaskName(16)));
    assertTrue(container0.getTasks().containsKey(getTaskName(18)));
    assertTrue(container0.getTasks().containsKey(getTaskName(2)));
    assertTrue(container0.getTasks().containsKey(getTaskName(3)));
    assertTrue(container0.getTasks().containsKey(getTaskName(5)));
    assertTrue(container0.getTasks().containsKey(getTaskName(7)));
    assertTrue(container0.getTasks().containsKey(getTaskName(9)));

    assertTrue(container1.getTasks().containsKey(getTaskName(1)));
    assertTrue(container1.getTasks().containsKey(getTaskName(11)));
    assertTrue(container1.getTasks().containsKey(getTaskName(13)));
    assertTrue(container1.getTasks().containsKey(getTaskName(15)));
    assertTrue(container1.getTasks().containsKey(getTaskName(17)));
    assertTrue(container1.getTasks().containsKey(getTaskName(19)));
    assertTrue(container1.getTasks().containsKey(getTaskName(20)));
    assertTrue(container1.getTasks().containsKey(getTaskName(4)));
    assertTrue(container1.getTasks().containsKey(getTaskName(6)));
    assertTrue(container1.getTasks().containsKey(getTaskName(8)));
  }
}
