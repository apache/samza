package org.apache.samza.sql.runner;

import org.apache.samza.context.ApplicationTaskContext;
import org.apache.samza.sql.translator.TranslatorContext;


public class SamzaSqlApplicationContext implements ApplicationTaskContext {
  private final TranslatorContext translatorContext;

  public SamzaSqlApplicationContext(TranslatorContext translatorContext) {
    this.translatorContext = translatorContext;
  }

  public TranslatorContext getTranslatorContext() {
    return translatorContext;
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }
}
