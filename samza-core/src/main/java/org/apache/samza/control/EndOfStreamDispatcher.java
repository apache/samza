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

package org.apache.samza.control;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.samza.control.EndOfStreamManager.EndOfStreamImpl;
import org.apache.samza.control.EndOfStreamManager.EndOfStreamState;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.util.IOGraphUtil;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class propagates the end-of-stream control messages.
 * It uses the the input/output of the StreamGraph to decide the output intermediate streams, and shuts down
 * the task if all inputs reach end-of-stream.
 */
public class EndOfStreamDispatcher {
  private static final Logger log = LoggerFactory.getLogger(EndOfStreamDispatcher.class);

  private final Multimap<SystemStream, IOGraphUtil.IONode> streamToIONodes = HashMultimap.create();

  /**
   * Create the dispatcher for end-of-stream messages based on the IOGraph built on StreamGraph
   * @param ioGraph the graph contains the topology info
   */
  public EndOfStreamDispatcher(Collection<IOGraphUtil.IONode> ioGraph) {
    ioGraph.forEach(node -> {
        node.getInputs().forEach(stream -> {
            streamToIONodes.put(new SystemStream(stream.getSystemName(), stream.getPhysicalName()), node);
          });
      });
  }

  /**
   * Propagate end-of-stream to any downstream intermediate streams which have not reached end-of-stream.
   * If all the inputs have been end-of-stream, shut down the current task.
   * a) streamsToTasks map in the EndOfStreamManager gives how many tasks are consuming the current input system streams
   * b) the number of tasks consuming the input system streams are now the upstream tasks that are sending EoS to the output
   *
   * @param endOfStream End-of-stream message
   * @param coordinator coordinator among tasks
   */
  public void propagate(EndOfStream endOfStream, TaskCoordinator coordinator) {
    EndOfStreamManager manager = ((EndOfStreamImpl) endOfStream).getManager();
    streamToIONodes.get(endOfStream.getSystemStream()).forEach(node -> {
        // find the intermediate streams that need broadcast the eos messages
        if (node.getOutputOpSpec().getOpCode() == OperatorSpec.OpCode.PARTITION_BY) {
          boolean inputsEndOfStream =
              node.getInputs().stream().allMatch(spec -> manager.isEndOfStream(spec.toSystemStream()));
          if (inputsEndOfStream) {
            // broadcast the end-of-stream message to the intermediate stream
            int count = (int) node.getInputs().stream()
                .flatMap(spec -> manager.getStreamToTasks().get(spec.toSystemStream()).stream()).collect(Collectors.toSet())
                .size();
            manager.sendEndOfStream(node.getOutput().toSystemStream(), count);
          }
        }
      });

    boolean allEndOfStream = manager.getEosStates().values().stream().allMatch(EndOfStreamState::isEndOfStream);
    if (allEndOfStream) {
      // all inputs have been end-of-stream, shut down the task
      log.info("All input streams have reached the end for task " + manager.getTaskName());
      coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
    }
  }
}
