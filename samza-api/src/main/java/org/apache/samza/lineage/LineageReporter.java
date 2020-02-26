package org.apache.samza.lineage;

/**
 * The LineageReporter interface defines how Samza writes job lineage information to outside systems, such as messaging
 * systems like Kafka, or file systems.
 * Implementations are responsible for accepting lineage data and writing them to their backend systems.
 */
public interface LineageReporter<T> {

  /**
   * Start the reporter.
   */
  void start();

  /**
   * Stop the reporter.
   */
  void stop();

  /**
   * Send the specified Samza job lineage data to outside system.
   * @param lineage Samza job lineage data model
   */
  void report(T lineage);
}
