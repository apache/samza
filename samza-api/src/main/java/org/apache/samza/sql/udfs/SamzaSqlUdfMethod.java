package org.apache.samza.sql.udfs;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.samza.sql.schema.SamzaSqlFieldType;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface SamzaSqlUdfMethod {
  SamzaSqlFieldType[] params() default {};
  SamzaSqlFieldType returns() default SamzaSqlFieldType.ANY;
}
