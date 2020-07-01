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
package org.apache.samza.example;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.example.models.AdClickEvent;
import org.apache.samza.example.models.EnrichedAdClickEvent;
import org.apache.samza.example.models.Member;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;


/**
 * An illustration of use of async APIs in high level application.
 * The following example demonstrates the use of {@link MessageStream#flatMapAsync(org.apache.samza.operators.functions.AsyncFlatMapFunction)}. We use a mock
 * member decorator service which returns a future in response to decorate request. Typically, in real world scenarios,
 * this mock member service will be replaced with rest call to a remote service.
 */
public class AsyncApplicationExample implements StreamApplication {

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor trackingSystem = new KafkaSystemDescriptor("tracking");

    KafkaInputDescriptor<AdClickEvent> inputStreamDescriptor =
        trackingSystem.getInputDescriptor("adClickEvent", new JsonSerdeV2<>(AdClickEvent.class));

    KafkaOutputDescriptor<KV<String, EnrichedAdClickEvent>> outputStreamDescriptor =
        trackingSystem.getOutputDescriptor("enrichedAdClickEvent",
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(EnrichedAdClickEvent.class)));

    MessageStream<AdClickEvent> adClickEventStream = appDescriptor.getInputStream(inputStreamDescriptor);
    OutputStream<KV<String, EnrichedAdClickEvent>> enrichedAdClickStream =
        appDescriptor.getOutputStream(outputStreamDescriptor);

    adClickEventStream
        .flatMapAsync(AsyncApplicationExample::enrichAdClickEvent)
        .map(enrichedAdClickEvent -> KV.of(enrichedAdClickEvent.getCountry(), enrichedAdClickEvent))
        .sendTo(enrichedAdClickStream);
  }

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner runner = ApplicationRunners.getApplicationRunner(new AsyncApplicationExample(), config);

    runner.run();
    runner.waitForFinish();
  }

  private static CompletionStage<Collection<EnrichedAdClickEvent>> enrichAdClickEvent(AdClickEvent adClickEvent) {
    CompletionStage<Member> decoratedMemberFuture = MemberDecoratorService.decorateMember(adClickEvent.getMemberId());
    return decoratedMemberFuture
        .thenApply(member -> Collections.singleton(
            new EnrichedAdClickEvent(adClickEvent.getId(), member.getGender(), member.getCountry())));
  }

  /**
   * A mock member decorator service that introduces delay to the member decorate call for illustrating async APIs
   * use in high level application. In real world, this component would correspond to a component that makes remote
   * calls.
   */
  private static class MemberDecoratorService {
    private static final String[] GENDER = {"F", "M", "U"};
    private static final List<String> COUNTRY = ImmutableList.of(
        "KENYA",
        "NEW ZEALAND",
        "INDONESIA",
        "PERU",
        "FRANCE",
        "MEXICO");
    private static final Random RANDOM = new Random();

    static CompletionStage<Member> decorateMember(int memberId) {
      return CompletableFuture.supplyAsync(() -> {
        /*
         * Introduce some lag to mimic remote call. In real use cases, this typically translates to over the wire
         * network call to some rest service.
         */
        try {
          Thread.sleep((long) (Math.random() * 10000));
        } catch (InterruptedException ec) {
          System.out.println("Interrupted during sleep");
        }

        return new Member(memberId, getRandomGender(), getRandomCountry());
      });
    }

    static String getRandomGender() {
      int index = RANDOM.nextInt(GENDER.length);
      return GENDER[index];
    }

    static String getRandomCountry() {
      int index = RANDOM.nextInt(COUNTRY.size());
      return COUNTRY.get(index);
    }
  }
}
