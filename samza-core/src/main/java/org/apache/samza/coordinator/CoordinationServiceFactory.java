package org.apache.samza.coordinator;

public interface CoordinationServiceFactory {
  CoordinationService getCoordinationService(String groupId);
}
