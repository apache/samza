/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.samza.sql.data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.Pair;
import org.apache.samza.SamzaException;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Defines a SQL row expression to a java class ({@link org.apache.samza.sql.data.Expression}) compiler.
 *
 * <p>This is based on Calcite's {@link org.apache.calcite.interpreter.JaninoRexCompiler}. This first generates
 * a Java AST and them compile it to a class using Janino.</p>
 */
public class RexToJavaCompiler {
  private static final Logger log = LoggerFactory.getLogger(RexToJavaCompiler.class);

  private final RexBuilder rexBuilder;

  public RexToJavaCompiler(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
  }

  /**
   * Compiles a relational expression to a instance of {@link Expression}
   *
   * for e.g.
   *    Query : select id from profile
   *      where profile table has relational schema with id(NUMBER) and name(VARCHAR) columns.
   *    This query will result in the following relational plan
   *      LogicalProject(id=[$1])
   *        LogicalTableScan(table=[[profile]])
   *
   *    And the corresponding expressions are
   *       inputs : EnumerableTableScan (Which is the output of LogicalTableScan)
   *       nodes : [$1] Which essentially means take pick the first column from the input
   *
   *
   *    This function converts the LogicalProject expression "[$1]" with input RexNode which is an output of TableScan
   *    to a java code that implements the interface {@link Expression}
   *
   * @param inputs Input relations/time-varying relations for this row expression
   * @param nodes relational expressions that needs to be converted to java code.
   * @return compiled expression of type {@link org.apache.samza.sql.data.Expression}
   */
  public org.apache.samza.sql.data.Expression compile(List<RelNode> inputs, List<RexNode> nodes) {
    /*
     *  In case there are multiple input relations, we build a single input row type combining types of all the inputs.
     */
    final RelDataTypeFactory.FieldInfoBuilder fieldBuilder = rexBuilder.getTypeFactory().builder();
    for (RelNode input : inputs) {
      fieldBuilder.addAll(input.getRowType().getFieldList());
    }
    final RelDataType inputRowType = fieldBuilder.build();
    final RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
    for (RexNode node : nodes) {
      programBuilder.addProject(node, null);
    }
    final RexProgram program = programBuilder.getProgram();

    final BlockBuilder builder = new BlockBuilder();
    final ParameterExpression executionContext = Expressions.parameter(SamzaSqlExecutionContext.class, "context");
    final ParameterExpression root = DataContext.ROOT;
    final ParameterExpression inputValues = Expressions.parameter(Object[].class, "inputValues");
    final ParameterExpression outputValues = Expressions.parameter(Object[].class, "outputValues");
    final JavaTypeFactoryImpl javaTypeFactory = new JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem());

    // public void execute(Object[] inputValues, Object[] outputValues)
    final RexToLixTranslator.InputGetter inputGetter = new RexToLixTranslator.InputGetterImpl(ImmutableList.of(
        Pair.<org.apache.calcite.linq4j.tree.Expression, PhysType>of(
            Expressions.variable(Object[].class, "inputValues"),
            PhysTypeImpl.of(javaTypeFactory, inputRowType, JavaRowFormat.ARRAY, false))));

    final List<org.apache.calcite.linq4j.tree.Expression> list =
        RexToLixTranslator.translateProjects(program, javaTypeFactory, builder, null, DataContext.ROOT, inputGetter,
            null);
    for (int i = 0; i < list.size(); i++) {
      builder.add(Expressions.statement(
          Expressions.assign(Expressions.arrayIndex(outputValues, Expressions.constant(i)), list.get(i))));
    }
    return createSamzaExpressionFromCalcite(executionContext, root, inputValues, outputValues, builder.toBlock());
  }

  /**
   * This method takes the java statement block, inputs, outputs needed by the statement block to create an object
   * of class that implements the interface {@link Expression}
   *
   * for e.g.
   *   Query : select id from profile
   *      where profile table has relational schema with id(NUMBER) and name(VARCHAR) columns.
   *    This query will result in the following relational plan
   *      LogicalProject(id=[$1])
   *        LogicalTableScan(table=[[profile]])
   *
   *
   *    And the corresponding expressions are
   *       inputs : EnumerableTableScan (Which is the output of LogicalTableScan)
   *       nodes : [$1] Which essentially means take pick the first column from the input
   *
   *    This expression corresponding to the logicalProject "[$1]" gets converted into a java statement block
   *    {
   *      outputValues[0] = (Integer) inputValues[1];
   *    }
   *
   *    This method converts this statement block into an equivalent {@link Expression} object whose execute methods
   *    execute the above java statement block
   *
   */
  static org.apache.samza.sql.data.Expression createSamzaExpressionFromCalcite(ParameterExpression executionContext,
      ParameterExpression dataContext, ParameterExpression inputValues, ParameterExpression outputValues,
      BlockStatement block) {
    final List<MemberDeclaration> declarations = Lists.newArrayList();

    // public void execute(Object[] inputValues, Object[] outputValues)
    declarations.add(
        Expressions.methodDecl(Modifier.PUBLIC, void.class, SamzaBuiltInMethod.EXPR_EXECUTE2.method.getName(),
            ImmutableList.of(executionContext, dataContext, inputValues, outputValues), block));

    final ClassDeclaration classDeclaration = Expressions.classDecl(Modifier.PUBLIC, "SqlExpression", null,
        ImmutableList.<Type>of(org.apache.samza.sql.data.Expression.class), declarations);
    String s = Expressions.toString(declarations, "\n", false);

    log.info("Generated code for expression: {}", s);

    try {
      return getExpression(classDeclaration, s);
    } catch (Exception e) {
      throw new SamzaException("Expression compilation failure.", e);
    }
  }

  /**
   * Creates the instance of the class defined in {@link ClassDeclaration}
   * @param expr Interface whose instance needs to be created.
   * @param s The java code that implements the interface which should be used to create the instance.
   * @return The object of the class which implements the interface {@link Expression} with the code that is passed as input.
   * @throws CompileException
   * @throws IOException
   */
  static Expression getExpression(ClassDeclaration expr, String s) throws CompileException, IOException {
    ICompilerFactory compilerFactory;
    try {
      compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
    } catch (Exception e) {
      throw new IllegalStateException("Unable to instantiate java compiler", e);
    }
    IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
    cbe.setClassName(expr.name);
    cbe.setImplementedInterfaces(expr.implemented.toArray(new Class[expr.implemented.size()]));
    cbe.setParentClassLoader(RexToJavaCompiler.class.getClassLoader());
    cbe.setDebuggingInformation(true, true, true);

    return (org.apache.samza.sql.data.Expression) cbe.createInstance(new StringReader(s));
  }

  /**
   * Represents the methods in the class {@link Expression}
   */
  public enum SamzaBuiltInMethod {
    EXPR_EXECUTE2(org.apache.samza.sql.data.Expression.class, "execute", SamzaSqlExecutionContext.class,
        DataContext.class, Object[].class, Object[].class);

    public final Method method;

    /**
     * Defines a method.
     */
    SamzaBuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
      this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }
  }
}

