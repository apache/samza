package org.apache.samza.serializers.model;

import java.util.Map;
import org.apache.samza.job.model.HostLocality;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * A mix-in Jackson class to convert {@link org.apache.samza.job.model.LocalityModel} to/from JSON
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class JsonLocalityModelMixIn {
  @JsonCreator
  public JsonLocalityModelMixIn(@JsonProperty("host-localities") Map<String, HostLocality> hostLocalities) {

  }

  @JsonProperty("host-localities")
  abstract Map<String, HostLocality> hostLocalities();
}
