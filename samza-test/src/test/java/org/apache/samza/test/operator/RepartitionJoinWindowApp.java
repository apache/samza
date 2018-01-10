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

package org.apache.samza.test.operator;

import joptsimple.OptionSet;
import org.apache.samza.application.ManagedApplicationMain;
import org.apache.samza.application.StreamApplications;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.application.ManagedApplicationMain.*;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.test.operator.data.AdClick;
import org.apache.samza.test.operator.data.PageView;
import org.apache.samza.test.operator.data.UserPageAdClick;

import java.time.Duration;


/**
 * A {@link StreamApplication} that demonstrates a partitionBy, stream-stream join and a windowed count.
 */
public class RepartitionJoinWindowApp {
  static final String PAGE_VIEWS = "page-views";
  static final String AD_CLICKS = "ad-clicks";
  static final String OUTPUT_TOPIC = "user-ad-click-counts";

  public static void main(String[] args) throws Exception {
    ApplicationMainCommandLine cmdLine = new ApplicationMainCommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    StreamApplication application = StreamApplications.createStreamApp(config);

    MessageStream<PageView> pageViews = application.openInput(PAGE_VIEWS, new JsonSerdeV2<>(PageView.class));
    MessageStream<AdClick> adClicks = application.openInput(AD_CLICKS, new JsonSerdeV2<>(AdClick.class));
    OutputStream<KV<String, String>> outputStream =
        application.openOutput(OUTPUT_TOPIC, new KVSerde<>(new StringSerde(), new StringSerde()));

    pageViews
        .partitionBy(PageView::getViewId, pv -> pv, new KVSerde<>(new StringSerde(), new JsonSerdeV2<>(PageView.class)))
        .map(KV::getValue)
        .join(adClicks
                .partitionBy(AdClick::getViewId, ac -> ac, new KVSerde<>(new StringSerde(), new JsonSerdeV2<>(AdClick.class)))
                .map(KV::getValue),
            new UserPageViewAdClicksJoiner(),
            new StringSerde(),
            new JsonSerdeV2<>(PageView.class),
            new JsonSerdeV2<>(AdClick.class),
            Duration.ofMinutes(1))
        .partitionBy(UserPageAdClick::getUserId, upac -> upac,
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(UserPageAdClick.class)))
        .map(KV::getValue)
        .window(Windows.keyedSessionWindow(UserPageAdClick::getUserId, Duration.ofSeconds(3), new StringSerde(), new JsonSerdeV2<>(UserPageAdClick.class)))
        .map(windowPane -> KV.of(windowPane.getKey().getKey(), String.valueOf(windowPane.getMessage().size())))
        .sendTo(outputStream);

    ApplicationMainOperation op = cmdLine.getOperation(options);
    ManagedApplicationMain.runCmd(application, op);
  }

  private static class UserPageViewAdClicksJoiner implements JoinFunction<String, PageView, AdClick, UserPageAdClick> {
    @Override
    public UserPageAdClick apply(PageView pv, AdClick ac) {
      return new UserPageAdClick(pv.getUserId(), pv.getPageId(), ac.getAdId());
    }

    @Override
    public String getFirstKey(PageView pv) {
      return pv.getViewId();
    }

    @Override
    public String getSecondKey(AdClick ac) {
      return ac.getViewId();
    }
  }
}
