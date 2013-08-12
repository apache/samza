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

package samza.examples.wikipedia.system;

import java.util.ArrayList;
import java.util.List;
import org.apache.samza.Partition;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedListener;

public class WikipediaConsumer extends BlockingEnvelopeMap implements WikipediaFeedListener {
  private final List<String> channels;
  private final String systemName;
  private final WikipediaFeed feed;

  public WikipediaConsumer(String systemName, WikipediaFeed feed, MetricsRegistry registry) {
    this.channels = new ArrayList<String>();
    this.systemName = systemName;
    this.feed = feed;
  }

  public void onEvent(final WikipediaFeedEvent event) {
    SystemStreamPartition systemStreamPartition = new SystemStreamPartition(systemName, event.getChannel(), new Partition(0));

    try {
      add(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, null, null, event));
    } catch (Exception e) {
      System.err.println(e);
    }
  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String startingOffset) {
    super.register(systemStreamPartition, startingOffset);

    channels.add(systemStreamPartition.getStream());
  }

  @Override
  public void start() {
    feed.start();

    for (String channel : channels) {
      feed.listen(channel, this);
    }
  }

  @Override
  public void stop() {
    for (String channel : channels) {
      feed.unlisten(channel, this);
    }

    feed.stop();
  }
}
