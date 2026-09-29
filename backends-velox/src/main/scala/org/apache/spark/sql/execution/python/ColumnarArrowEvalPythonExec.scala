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

import org.apache.spark.{ContextAwareIterator, JobArtifactSet, SparkException, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.convention.{BatchType, Convention, ConventionReq, RowType}
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{DataType, StructField, StructType, UserDefinedType}
import org.apache.spark.sql.types.DataType.equalsIgnoreCompatibleCollation
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * Evaluates Arrow Python UDFs on Arrow columnar input, like Spark's [[ArrowEvalPythonExec]] with
 * SPARK-56350 (Arrow-backed columnar input), but without any row conversion.
 *
 * It only uses Spark APIs: it consumes and produces `BatchType.ArrowBatchType` (SPARK-57468), and
 * the plugin inserts the transitions from / to its own columnar format around it.
 *
 *   - The UDF input columns are serialized to the Python worker directly from their Arrow vectors,
 *     by Spark's columnar Arrow Python runner.
 *   - All input columns are kept, zero-copy, until the corresponding Python output arrives, then
 *     emitted together with the UDF result columns. An output batch is valid until the next one is
 *     requested.
 */
case class ColumnarArrowEvalPythonExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)
  extends UnaryExecNode
  with PythonSQLMetrics {

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)
  private[this] val sessionUUID = {
    Option(session).collect {
      case session if session.sessionState.conf.pythonWorkerLoggingEnabled =>
        session.sessionUUID
    }
  }

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override def supportsColumnar: Boolean = true

  override def supportsRowBased: Boolean = false

  override def convention: Convention = Convention(RowType.None, BatchType.ArrowBatchType)

  override def requiredChildConventions(outputsColumnar: Boolean): Seq[ConventionReq] =
    Seq(ConventionReq.Batch(BatchType.ArrowBatchType))

  override protected def doExecute(): RDD[InternalRow] =
    throw SparkException.internalError(s"$nodeName does not support row-based execution")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val (pyFuncs, inputs) = udfs.map(ColumnarArrowEvalPythonExec.collectFunctions).unzip
    val allInputs = new ArrayBuffer[Expression]
    val argMetas = inputs.map {
      input =>
        input.map {
          e =>
            val (key, value) = e match {
              case NamedArgumentExpression(key, value) => (Some(key), value)
              case _ => (None, e)
            }
            val index = allInputs.indexWhere(_.semanticEquals(value))
            if (index >= 0) {
              ArgumentMetadata(index, key)
            } else {
              allInputs += value
              ArgumentMetadata(allInputs.length - 1, key)
            }
        }.toArray
    }.toArray
    val inputOrdinals = allInputs.map {
      case a: Attribute => child.output.indexWhere(_.exprId == a.exprId)
      case e => throw SparkException.internalError(s"UDF input is not an attribute: $e")
    }.toArray
    val udfInputSchema = StructType(allInputs.zipWithIndex.map {
      case (e, i) => StructField(s"_$i", e.dataType)
    }.toSeq)
    val outputTypes = resultAttrs.map(_.dataType)
    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val largeVarTypes = conf.arrowUseLargeVarTypes
    val runnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
    val pythonMetrics = this.pythonMetrics
    val jobArtifactUUID = this.jobArtifactUUID
    val sessionUUID = this.sessionUUID

    child.executeColumnar().mapPartitionsInternal {
      iter =>
        val context = TaskContext.get()
        // Input batches (owned by this operator) waiting for their Python output.
        val pending = new java.util.ArrayDeque[ColumnarBatch]()
        var lastOutput: ColumnarBatch = null
        context.addTaskCompletionListener[Unit] {
          _ =>
            if (lastOutput != null) {
              lastOutput.close()
            }
            pending.asScala.foreach(_.close())
            pending.clear()
        }

        val udfInput = new ContextAwareIterator(context, iter).map {
          batch =>
            val owned = ColumnarArrowEvalPythonExec.takeOwnership(batch)
            pending.add(owned)
            owned
        }
        // Spark's columnar Arrow runner (SPARK-56350) serializes the UDF input columns of
        // Arrow-backed batches as they are.
        val runner = new ColumnarArrowPythonWithNamedArgumentRunner(
          pyFuncs,
          evalType,
          argMetas,
          udfInputSchema,
          sessionLocalTimeZone,
          largeVarTypes,
          runnerConf,
          pythonMetrics,
          jobArtifactUUID,
          sessionUUID,
          inputOrdinals)
        runner.compute(udfInput, context.partitionId(), context).map {
          result =>
            val actualTypes = (0 until result.numCols).map(result.column(_).dataType)
            if (!equalsIgnoreCompatibleCollation(outputTypes, actualTypes)) {
              throw SparkException.internalError(
                s"Invalid schema from Arrow Python UDF: expected ${outputTypes.mkString(", ")}, " +
                  s"got ${actualTypes.mkString(", ")}")
            }
            // The previous output has been consumed.
            if (lastOutput != null) {
              lastOutput.close()
            }
            val input = pending.poll()
            if (input == null || input.numRows != result.numRows) {
              throw SparkException.internalError(
                s"Python output has ${result.numRows} rows, input has " +
                  s"${Option(input).map(_.numRows)} rows")
            }
            lastOutput = input
            val columns = (0 until input.numCols).map(input.column) ++
              (0 until result.numCols).map(result.column)
            new ColumnarBatch(columns.toArray, result.numRows)
        }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarArrowEvalPythonExec =
    copy(child = newChild)
}

object ColumnarArrowEvalPythonExec {

  /** Whether the UDFs can be evaluated by [[ColumnarArrowEvalPythonExec]] on `child`. */
  def isSupported(udfs: Seq[PythonUDF], child: SparkPlan): Boolean = {
    def hasUdt(dt: DataType): Boolean = dt.existsRecursively(_.isInstanceOf[UserDefinedType[_]])
    val inputs = udfs.flatMap(collectFunctions(_)._2).map {
      case NamedArgumentExpression(_, value) => value
      case e => e
    }
    inputs.forall {
      case a: AttributeReference => child.output.exists(_.exprId == a.exprId)
      case _ => false
    } && !inputs.exists(e => hasUdt(e.dataType)) && !udfs.exists(u => hasUdt(u.dataType))
  }

  def collectFunctions(udf: PythonUDF): ((ChainedPythonFunctions, Long), Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val ((chained, _), children) = collectFunctions(u)
        ((ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), udf.resultId.id), children)
      case children =>
        // There should be no PythonUDF, or the children can't be evaluated directly.
        assert(!children.exists(_.isInstanceOf[PythonUDF]))
        ((ChainedPythonFunctions(Seq(udf.func)), udf.resultId.id), udf.children)
    }
  }

  /**
   * Moves the Arrow vectors of `batch` into a new batch owned by the caller, without copying. The
   * producer's vectors are left empty.
   */
  private def takeOwnership(batch: ColumnarBatch): ColumnarBatch = {
    val columns = (0 until batch.numCols).map {
      i =>
        val vector = batch.column(i).asInstanceOf[ArrowColumnVector].getValueVector
        val pair = vector.getTransferPair(vector.getAllocator)
        pair.transfer()
        new ArrowColumnVector(pair.getTo).asInstanceOf[ColumnVector]
    }
    new ColumnarBatch(columns.toArray, batch.numRows)
  }
}

object PullOutArrowEvalPythonPreProjectHelper extends PullOutProjectHelper {

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
    val (_, inputs) =
      arrowEvalPythonExec.udfs.map(ColumnarArrowEvalPythonExec.collectFunctions).unzip
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
