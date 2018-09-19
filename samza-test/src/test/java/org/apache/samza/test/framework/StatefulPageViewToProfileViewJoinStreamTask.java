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

package org.apache.samza.test.framework;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.table.TestTableData;

public class StatefulPageViewToProfileViewJoinStreamTask implements StreamTask, InitableTask {
  private KeyValueStore<String, TestTableData.PageView> pageViewStore;
  private KeyValueStore<String, TestTableData.Profile> profileViewStore;

  public void init(Config config, TaskContext context) {
    this.pageViewStore = (KeyValueStore<String, TestTableData.PageView>) context.getStore("page-view-store");
    this.profileViewStore = (KeyValueStore<String, TestTableData.Profile>) context.getStore("profile-store");

  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {

    if (envelope.getMessage() instanceof TestTableData.Profile) {
      TestTableData.Profile profileView = (TestTableData.Profile) envelope.getMessage();
      TestTableData.PageView pageView = pageViewStore.get(Integer.toString(profileView.getMemberId()));
      if(pageView != null) {
        System.out.println(pageView.getPageKey()+"==="+ profileView.getMemberId()+"==="+profileView.getCompany());
        collector.send(new OutgoingMessageEnvelope(new SystemStream("test", "output"),
            new TestTableData.EnrichedPageView(pageView.getPageKey(), profileView.getMemberId(), profileView.getCompany())));
      } else {
        profileViewStore.put(Integer.toString(profileView.getMemberId()), profileView);
      }
    }

    else if (envelope.getMessage() instanceof TestTableData.PageView) {
      TestTableData.PageView pageView = (TestTableData.PageView) envelope.getMessage();
      TestTableData.Profile profileView = profileViewStore.get(Integer.toString(pageView.getMemberId()));
      if(profileView != null) {
        System.out.println(pageView.getPageKey()+"==="+ profileView.getMemberId()+"==="+profileView.getCompany());
        collector.send(new OutgoingMessageEnvelope(new SystemStream("test", "output"),
            new TestTableData.EnrichedPageView(pageView.getPageKey(), profileView.getMemberId(), profileView.getCompany())));
      } else {
        pageViewStore.put(Integer.toString(pageView.getMemberId()), pageView);
      }
    }
  }
}
