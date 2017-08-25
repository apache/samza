package org.apache.samza.operators.functions;

import java.io.Serializable;


/**
 * Created by yipan on 8/21/17.
 */
public interface OperatorBiFunction<T, T1, T2> extends InitableFunction, ClosableFunction, Serializable {
  T2 apply(T t, T1 t1);
}
