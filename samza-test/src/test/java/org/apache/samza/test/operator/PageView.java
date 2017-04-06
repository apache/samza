package org.apache.samza.test.operator;

  class PageView {
    private final String userId;
    private final String country;
    private final String url;

    PageView(String message) {
      String[] pageViewFields = message.split(",");
      userId = pageViewFields[0];
      country = pageViewFields[1];
      url = pageViewFields[2];
    }

    String getUserId() {
      return userId;
    }

    String getCountry() {
      return country;
    }

    String getUrl() {
      return url;
    }

  }
