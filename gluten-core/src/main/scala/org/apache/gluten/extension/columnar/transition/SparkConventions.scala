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
package org.apache.gluten.extension.columnar.transition

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.convention.{BatchType => SparkBatchType, ConventionReq => SparkConventionReq, RowType => SparkRowType}

import java.util.concurrent.ConcurrentHashMap

/**
 * Bridges Spark's convention API (SPARK-57468) and Gluten's [[Convention]].
 *
 * A Spark row / batch type can be bound to a Gluten one. A vanilla Spark plan that declares a
 * bound, non-vanilla Spark type (e.g. an operator consuming and producing
 * `SparkBatchType.ArrowBatchType`) is then planned by Gluten like any other plan of the Gluten
 * type: Gluten inserts its own transitions around it, with no operator-specific code.
 *
 * Gluten plans themselves keep reporting vanilla conventions to Spark (derived from
 * `supportsRowBased` / `supportsColumnar`): Spark's transition insertion only runs on the plan
 * before Gluten's rules are applied.
 */
object SparkConventions {
  private val rowTypes = new ConcurrentHashMap[SparkRowType, Convention.RowType]()
  private val batchTypes = new ConcurrentHashMap[SparkBatchType, Convention.BatchType]()

  bind(SparkRowType.VanillaRowType, Convention.RowType.VanillaRowType)
  bind(SparkBatchType.VanillaBatchType, Convention.BatchType.VanillaBatchType)

  def bind(spark: SparkRowType, gluten: Convention.RowType): Unit = {
    rowTypes.put(spark, gluten)
  }

  def bind(spark: SparkBatchType, gluten: Convention.BatchType): Unit = {
    batchTypes.put(spark, gluten)
  }

  private def isVanilla(t: SparkRowType): Boolean =
    t == SparkRowType.None || t == SparkRowType.VanillaRowType

  private def isVanilla(t: SparkBatchType): Boolean =
    t == SparkBatchType.None || t == SparkBatchType.VanillaBatchType

  def rowTypeOf(t: SparkRowType): Convention.RowType = {
    if (t == SparkRowType.None) {
      return Convention.RowType.None
    }
    Option(rowTypes.get(t)).getOrElse {
      throw new IllegalStateException(s"Spark row type $t is not bound to a Gluten row type")
    }
  }

  def batchTypeOf(t: SparkBatchType): Convention.BatchType = {
    if (t == SparkBatchType.None) {
      return Convention.BatchType.None
    }
    Option(batchTypes.get(t)).getOrElse {
      throw new IllegalStateException(s"Spark batch type $t is not bound to a Gluten batch type")
    }
  }

  /** Whether a vanilla plan declares a non-vanilla Spark row type. */
  def hasCustomRowType(plan: SparkPlan): Boolean = !isVanilla(plan.convention.rowType)

  /** Whether a vanilla plan declares a non-vanilla Spark batch type. */
  def hasCustomBatchType(plan: SparkPlan): Boolean = !isVanilla(plan.convention.batchType)

  private def childReqsOf(plan: SparkPlan): Seq[SparkConventionReq] =
    plan.requiredChildConventions(plan.convention.supportsBatch)

  /** Whether a vanilla plan requires non-vanilla Spark conventions from its children. */
  def hasCustomChildReqs(plan: SparkPlan): Boolean = childReqsOf(plan).exists {
    case SparkConventionReq.Row(t) => !isVanilla(t)
    case SparkConventionReq.Batch(t) => !isVanilla(t)
    case SparkConventionReq.Any => false
  }

  def childReqsInGluten(plan: SparkPlan): Seq[ConventionReq] = childReqsOf(plan).map {
    case SparkConventionReq.Row(t) => ConventionReq.ofRow(ConventionReq.RowType.Is(rowTypeOf(t)))
    case SparkConventionReq.Batch(t) =>
      ConventionReq.ofBatch(ConventionReq.BatchType.Is(batchTypeOf(t)))
    case SparkConventionReq.Any => ConventionReq.any
  }
}
