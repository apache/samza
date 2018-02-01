package org.apache.samza.test.timer;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.test.operator.data.PageView;
import org.apache.samza.test.util.StreamAssert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by xiliu on 2/1/18.
 */
public class TestTimerApp implements StreamApplication {
  public static final String PAGE_VIEWS = "page-views";

  @Override
  public void init(StreamGraph graph, Config config) {
    final JsonSerdeV2<PageView> serde = new JsonSerdeV2<>(PageView.class);
    final MessageStream<PageView> pageViews = graph.getInputStream(PAGE_VIEWS, serde);
    final MessageStream<PageView> output = pageViews.flatMap(new FlatmapTimerFn());

    StreamAssert.that("Output from timer function should container all complete messages", output, serde)
        .containsInAnyOrder(
            Arrays.asList(
                new PageView("v1-complete", "p1", "u1"),
                new PageView("v2-complete", "p2", "u1"),
                new PageView("v3-complete", "p1", "u2"),
                new PageView("v4-complete", "p3", "u2")
            ));
  }

  private static class FlatmapTimerFn implements FlatMapFunction<PageView, PageView>, TimerFunction<String, PageView> {

    private List<PageView> pageViews = new ArrayList<>();
    private TimerRegistry<String> timerRegistry;

    @Override
    public Collection<PageView> apply(PageView message) {
      final PageView pv = new PageView(message.getViewId() + "-complete", message.getPageId(), message.getUserId());
      pageViews.add(pv);

      if (pageViews.size() == 2) {
        //got all messages for this task
        timerRegistry.register("CompleteTimer", 100);
      }
      return Collections.emptyList();
    }

    @Override
    public void initTimers(TimerRegistry<String> timerRegistry) {
      this.timerRegistry = timerRegistry;
    }

    @Override
    public Collection<PageView> onTimer(String key) {
      return pageViews;
    }
  }
}
