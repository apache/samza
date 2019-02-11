package org.apache.samza.sql.util;

import org.apache.samza.config.Config;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.udfs.SamzaSqlUdf;
import org.apache.samza.sql.udfs.SamzaSqlUdfMethod;
import org.apache.samza.sql.udfs.ScalarUdf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * UDF to test polymorphism.
 */
@SamzaSqlUdf(name = "MyTestPoly", description = "Test Polymorphism UDF.")
public class MyTestPolyUdf implements ScalarUdf {
  private static final Logger LOG = LoggerFactory.getLogger(MyTestPolyUdf.class);

  @SamzaSqlUdfMethod(params = SamzaSqlFieldType.INT32)
  public Integer execute(Integer value) {
    return value * 2;
  }

  @SamzaSqlUdfMethod(params = SamzaSqlFieldType.ANY)
  public Integer execute(String value) {
    return value.length() * 2;
  }


  @Override
  public void init(Config udfConfig) {
    LOG.info("Init called with {}", udfConfig);
  }
}
