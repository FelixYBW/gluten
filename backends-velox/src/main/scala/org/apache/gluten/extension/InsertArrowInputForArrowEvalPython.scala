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
package org.apache.gluten.extension

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.ArrowJavaBatchType
import org.apache.gluten.backendsapi.velox.VeloxBatchType
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.columnar.transition.{Convention, Transitions}

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.convention.{BatchType => SparkBatchType}
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec

/**
 * Lets Spark's [[ArrowEvalPythonExec]] evaluate Arrow Python UDFs on the Arrow data of a Velox
 * child: the child is loaded as `ArrowJavaBatchType`, which Gluten plans report to Spark as
 * `ArrowBatchType` (SPARK-57468). The exec then consumes and produces Arrow batches, and
 * [[org.apache.gluten.extension.columnar.transition.InsertTransitions]], which must run after this
 * rule, offloads its output back to Velox.
 *
 * The exec is left as it is when Spark doesn't evaluate the UDFs on Arrow batches, e.g. when a UDF
 * input is not a column of the child.
 */
case class InsertArrowInputForArrowEvalPython() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableColumnarArrowUDF) {
      return plan
    }
    plan.transformUp {
      case p: ArrowEvalPythonExec
          if p.convention.batchType != SparkBatchType.ArrowBatchType &&
            Convention.get(p.child).batchType == VeloxBatchType =>
        val arrowInput =
          p.withNewChildren(Seq(Transitions.toBatchPlan(p.child, ArrowJavaBatchType)))
        if (arrowInput.convention.batchType == SparkBatchType.ArrowBatchType) arrowInput else p
    }
  }
}
