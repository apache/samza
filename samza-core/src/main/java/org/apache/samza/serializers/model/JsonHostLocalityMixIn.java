package org.apache.samza.serializers.model;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * A mix-in Jackson class to convert {@link org.apache.samza.job.model.HostLocality} to/from JSON
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class JsonHostLocalityMixIn {
  @JsonCreator
  public JsonHostLocalityMixIn(@JsonProperty("id") String id, @JsonProperty("host") String host,
      @JsonProperty("jmx-url") String jmxUrl, @JsonProperty("jmx-tunneling-url") String jmxTunnelingUrl) {
  }

  @JsonProperty("id")
  abstract String id();

  @JsonProperty("host")
  abstract String host();

  @JsonProperty("jmx-url")
  abstract String jmxUrl();

  @JsonProperty("jmx-tunneling-url")
  abstract String jmxTunnelingUrl();
}
