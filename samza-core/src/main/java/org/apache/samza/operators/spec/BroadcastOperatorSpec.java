package org.apache.samza.operators.spec;

import org.apache.samza.operators.functions.WatermarkFunction;

/**
 * Created by xiliu on 1/16/18.
 */
public class BroadcastOperatorSpec<M> extends OperatorSpec<M, Void> {
  private final OutputStreamImpl<M> outputStream;


  public BroadcastOperatorSpec(OutputStreamImpl<M> outputStream, String opId) {
    super(OpCode.BROADCAST, opId);

    this.outputStream = outputStream;
  }

  public OutputStreamImpl<M> getOutputStream() {
    return this.outputStream;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return null;
  }
}
