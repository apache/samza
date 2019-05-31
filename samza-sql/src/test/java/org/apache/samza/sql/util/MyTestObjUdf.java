package org.apache.samza.sql.util;

import org.apache.samza.config.Config;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.udfs.SamzaSqlUdf;
import org.apache.samza.sql.udfs.SamzaSqlUdfMethod;
import org.apache.samza.sql.udfs.ScalarUdf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test UDF used by unit and integration tests.
 */
@SamzaSqlUdf(name = "MyTestObj", description = "Test UDF.")
public class MyTestObjUdf implements ScalarUdf {

  private static final Logger LOG = LoggerFactory.getLogger(MyTestUdf.class);

  @SamzaSqlUdfMethod(params = SamzaSqlFieldType.INT32)
  public Object execute(Integer value) {
    return value;
  }

  @Override
  public void init(Config udfConfig) {
    LOG.info("Init called with {}", udfConfig);
  }
}
