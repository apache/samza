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

function planToDagre(data) {
  // Create the input graph
  var g = new dagreD3.graphlib.Graph()
      .setGraph({
        rankdir: "LR"
      })
      .setDefaultEdgeLabel(function() { return {}; });

  var allStreams = [data.sourceStreams, data.sinkStreams, data.intermediateStreams];
  var streamClasses = ["source", "sink", "intermediate"];
  for (var i = 0; i < allStreams.length; i++) {
    var streams = allStreams[i];
    for (var streamId in streams) {
      var stream = streams[streamId];
      var labelVal = "<div><h3 class=\"topbar\">" + stream.streamSpec.id + "</h3><ul class=\"detailBox\" >"
      labelVal += "<li>SystemName: " + stream.streamSpec.systemName + "</li>"
      labelVal += "<li>PhysicalName: " + stream.streamSpec.physicalName + "</li>"
      labelVal += "<li>PartitionCount: " + stream.streamSpec.partitionCount + "</li>"
      labelVal += "</ul></div>"
      g.setNode(streamId,  { label: labelVal, labelType: "html", shape: "ellipse", class: streamClasses[i] });
    }
  }

  var canonicalId = {};
  var jobs = data.jobs;
  for (var i = 0; i < jobs.length; i++) {
    var operators = jobs[i].operatorGraph.operators;
    for (var opId in operators) {
      var operator = operators[opId];
      var labelVal = "<div><h3 class=\"topbar\">" + operator.opCode + "</h3><ul class=\"detailBox\">";
      var opId = operator.opId;
      if (parseInt(operator.pairedOpId) != -1) {
        opId = parseInt(operator.opId) < parseInt(operator.pairedOpId)?
            (operator.opId + "," + operator.pairedOpId) :
            (operator.pairedOpId + "," + operator.opId);
      }
      canonicalId[operator.opId] = opId;
      labelVal +=  "<li>ID: " + opId + "</li>";
      labelVal +=  "<li>@" + operator.caller + "</li>";
      labelVal += "</ul></div>";
      g.setNode(opId,  { label: labelVal, labelType: "html", rx: 5, ry: 5 });
    }
  }

  for (var i = 0; i < jobs.length; i++) {
    var inputs = jobs[i].operatorGraph.inputStreams;
    for (var k = 0; k < inputs.length; k++) {
      var input = inputs[k];
      for (var m = 0; m < input.nextOperatorIds.length; m++) {
        g.setEdge(input.streamId, canonicalId[input.nextOperatorIds[m].toString()]);
      }
    }

    var operators = jobs[i].operatorGraph.operators;
    for (var opId in operators) {
      var operator = operators[opId];
      for (var j = 0; j < operator.nextOperatorIds.length; j++) {
        g.setEdge(canonicalId[opId], canonicalId[operator.nextOperatorIds[j].toString()]);
      }
      if (operator.outputStreamId !== null) {
        g.setEdge(canonicalId[opId], operator.outputStreamId);
      }
    }
  }

  return g;
}
