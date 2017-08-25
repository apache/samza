package org.apache.samza.operators.functions;

import java.io.Serializable;


/**
 * Created by yipan on 8/21/17.
 */
public interface SupplierFunction<T> extends InitableFunction, ClosableFunction, Serializable {
  T get();
}
