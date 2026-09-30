/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.python

import org.apache.gluten.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Pulls the UDF inputs of an [[ArrowEvalPythonExec]] that are not columns out into a pre-project,
 * so Spark evaluates the UDFs on the Arrow vectors of its child.
 */
object PullOutArrowEvalPythonPreProjectHelper extends PullOutProjectHelper {

  private def inputsOf(udf: PythonUDF): Seq[Expression] = udf.children match {
    case Seq(u: PythonUDF) => inputsOf(u)
    case children =>
      // There should be no PythonUDF, or the children can't be evaluated directly.
      assert(!children.exists(_.isInstanceOf[PythonUDF]))
      children
  }

  private def rewriteUDF(
      udf: PythonUDF,
      expressionMap: mutable.HashMap[Expression, NamedExpression]): PythonUDF = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        udf
          .withNewChildren(udf.children.toIndexedSeq.map {
            func => rewriteUDF(func.asInstanceOf[PythonUDF], expressionMap)
          })
          .asInstanceOf[PythonUDF]
      case children =>
        val newUDFChildren = udf.children.map {
          case literal: Literal => literal
          case other => replaceExpressionWithAttribute(other, expressionMap)
        }
        udf.withNewChildren(newUDFChildren).asInstanceOf[PythonUDF]
    }
  }

  def pullOutPreProject(arrowEvalPythonExec: ArrowEvalPythonExec): SparkPlan = {
    // pull out preproject
    val inputs = arrowEvalPythonExec.udfs.map(inputsOf)
    val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
    // flatten all the arguments
    val allInputs = new ArrayBuffer[Expression]
    for (input <- inputs) {
      input.foreach {
        e =>
          if (!allInputs.exists(_.semanticEquals(e))) {
            allInputs += e
            replaceExpressionWithAttribute(e, expressionMap)
          }
      }
    }
    if (!expressionMap.isEmpty) {
      // Need preproject.
      val preProject = ProjectExec(
        eliminateProjectList(arrowEvalPythonExec.child.outputSet, expressionMap.values.toSeq),
        arrowEvalPythonExec.child)
      val newUDFs = arrowEvalPythonExec.udfs.map(f => rewriteUDF(f, expressionMap))
      val newArrowEvalPythonExec = arrowEvalPythonExec.copy(udfs = newUDFs, child = preProject)
      newArrowEvalPythonExec.copyTagsFrom(arrowEvalPythonExec)
      newArrowEvalPythonExec
    } else {
      arrowEvalPythonExec
    }
  }
}
