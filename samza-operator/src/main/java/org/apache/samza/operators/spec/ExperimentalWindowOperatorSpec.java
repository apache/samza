package org.apache.samza.operators.spec;

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.operators.windows.experimental.WindowDefinition;

/**
 *
 */
public class ExperimentalWindowOperatorSpec<M extends MessageEnvelope, K, WK, WV, WM extends WindowOutput<WK, WV>> implements OperatorSpec<WM>{

  private final WindowDefinition window;

  /**
   * The output {@link MessageStream}.
   */
  private final MessageStreamImpl<WM> outputStream;

  private final String operatorId;


  public ExperimentalWindowOperatorSpec(WindowDefinition window, String operatorId) {
    this.window = window;
    this.outputStream = new MessageStreamImpl<>();
    this.operatorId = operatorId;
  }

  @Override
  public MessageStream<WM> getOutputStream() {
    return this.outputStream;
  }

  public WindowDefinition getWindow() {
    return window;
  }

  public String getOperatorId() {
    return operatorId;
  }
}
