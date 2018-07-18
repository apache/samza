package org.apache.samza.application.internal;

import java.lang.reflect.Constructor;
import java.util.Collection;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationInitializer;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.runtime.internal.StreamApplicationSpec;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.Table;


/**
 * Created by yipan on 7/10/18.
 */
public class StreamApplicationBuilder extends ApplicationBuilder<StreamApplication> implements StreamApplicationInitializer, StreamApplicationSpec {
  final StreamApplicationBuilder graphBuilder;

  public StreamApplicationBuilder(StreamApplication userApp, Config config) {
    super(userApp, config);
    this.graphBuilder = createDefault(config);
    userApp.init(this, config);
  }

  private StreamApplicationBuilder createDefault(Config config) {
    try {
      Constructor<?> constructor = Class.forName("org.apache.samza.operators.StreamGraphSpec").getConstructor(Config.class); // *sigh*
      return (StreamApplicationBuilder) constructor.newInstance(config);
    } catch (Exception e) {
      throw new SamzaException("Cannot instantiate an empty StreamGraph to start user application.", e);
    }
  }

  @Override
  public void setDefaultSerde(Serde<?> serde) {
    this.graphBuilder.setDefaultSerde(serde);
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId, Serde<M> serde) {
    return this.graphBuilder.getInputStream(streamId, serde);
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId) {
    return this.graphBuilder.getInputStream(streamId);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId, Serde<M> serde) {
    return this.graphBuilder.getOutputStream(streamId, serde);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId) {
    return this.graphBuilder.getOutputStream(streamId);
  }

  @Override
  public <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDesc) {
    return this.graphBuilder.getTable(tableDesc);
  }

  @Override
  public StreamApplicationInitializer withContextManager(ContextManager contextManager) {
    return this.graphBuilder.withContextManager(contextManager);
  }

  @Override
  public ContextManager getContextManager() {
    return this.graphBuilder.getContextManager();
  }

  @Override
  public Collection<String> getInputStreams() {
    return this.graphBuilder.getInputStreams();
  }

  @Override
  public Collection<String> getOutputStreams() {
    return this.graphBuilder.getOutputStreams();
  }

  @Override
  public Collection<String> getBroadcastStreams() {
    return this.graphBuilder.getBroadcastStreams();
  }

  @Override
  public Collection<String> getTables() {
    return this.graphBuilder.getTables();
  }
}
