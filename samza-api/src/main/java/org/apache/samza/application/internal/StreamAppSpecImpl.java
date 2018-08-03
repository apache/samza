package org.apache.samza.application.internal;

import java.lang.reflect.Constructor;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationSpec;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.Table;


/**
 * Created by yipan on 7/10/18.
 */
public class StreamAppSpecImpl extends AppSpecImpl<StreamApplication, StreamApplicationSpec> implements StreamApplicationSpec {
  final StreamGraph graph;

  public StreamAppSpecImpl(StreamApplication userApp, Config config) {
    super(config);
    this.graph = createDefaultGraph(config);
    userApp.describe(this);
  }

  private StreamApplicationSpec createDefaultGraph(Config config) {
    try {
      Constructor<?> constructor = Class.forName("org.apache.samza.operators.StreamGraphSpec").getConstructor(Config.class); // *sigh*
      return (StreamApplicationSpec) constructor.newInstance(config);
    } catch (Exception e) {
      throw new SamzaException("Cannot instantiate an empty StreamGraph to start user application.", e);
    }
  }

  @Override
  public void setDefaultSerde(Serde<?> serde) {
    this.graph.setDefaultSerde(serde);
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId, Serde<M> serde) {
    return this.graph.getInputStream(streamId, serde);
  }

  @Override
  public <M> MessageStream<M> getInputStream(String streamId) {
    return this.graph.getInputStream(streamId);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId, Serde<M> serde) {
    return this.graph.getOutputStream(streamId, serde);
  }

  @Override
  public <M> OutputStream<M> getOutputStream(String streamId) {
    return this.graph.getOutputStream(streamId);
  }

  @Override
  public <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDesc) {
    return this.graph.getTable(tableDesc);
  }

  public StreamGraph getGraph() {
    return graph;
  }

}
