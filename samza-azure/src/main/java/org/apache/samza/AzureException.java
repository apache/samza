package org.apache.samza;

public class AzureException extends RuntimeException {

  public AzureException() {
    super();
  }

  public AzureException(String s, Throwable t) {
    super(s, t);
  }

  public AzureException(String s) {
    super(s);
  }

  public AzureException(Throwable t) {
    super(t);
  }

}
