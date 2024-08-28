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
package org.apache.gluten.execution

import org.apache.gluten.GlutenConfig
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.extension.{GlutenPlan, ValidationResult}
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.iterator.Iterators
import org.apache.gluten.vectorized.ArrowWritableColumnVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, MutableProjection, NamedExpression, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{ExplainUtils, OrderPreservingNodeShim, PartitioningPreservingNodeShim, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.vectorized.{MutableColumnarRow, WritableColumnVector}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType, TimestampType, YearMonthIntervalType}
import org.apache.spark.sql.utils.SparkSchemaUtil
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

case class ProjectColumnarExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode
  with PartitioningPreservingNodeShim
  with OrderPreservingNodeShim
  with GlutenPlan {

  @transient override lazy val metrics = Map(
    "time" -> SQLMetrics.createTimingMetric(sparkContext, "time of project"),
    "column_to_row_time" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of velox to Arrow ColumnarBatch"),
    "row_to_column_time" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of Arrow ColumnarBatch to velox")
  )

  private val (mutableProjectList, projectIndexInChild, projectIndexes) = removeAttributeReferenceFromProjectList()

  private val mutableProjectOutput = mutableProjectList.map(_.toAttribute)

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = projectList

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def supportsColumnar: Boolean = true



  // Only the mutable data type is valid
  private def validateDataType(dataType: DataType): Boolean = {
    dataType match {
      case _: BooleanType => true
      case _: ByteType => true
      case _: ShortType => true
      case _: IntegerType => true
      case _: LongType => true
      case _: FloatType => true
      case _: DoubleType => true
      case _: TimestampType => true
      case _: DateType => true
      case _: DecimalType => true
      case _ => false
    }
  }

  // Return the expression in projectList to MutableProjection
  // Return the index in child output
  // Return the index in this operator output
  private def removeAttributeReferenceFromProjectList(): (Seq[NamedExpression], Seq[Int], Seq[Int]) = {
    val childOutput = child.output
    val (attrs, notAttrs) = projectList.zipWithIndex.partition(e => e._1.isInstanceOf[AttributeReference])
    (notAttrs.map(_._1), attrs.map( a => childOutput.indexWhere(a._1.exprId == _.exprId)), attrs.map(_._2))
  }

//  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
//    val timeMetric = longMetric("time")
//    val c2rTime = longMetric("column_to_row_time")
//    val r2cTime = longMetric("row_to_column_time")
//    child.executeColumnar().mapPartitions {
//      batches =>
//        val res: Iterator[Iterator[ColumnarBatch]] = new Iterator[Iterator[ColumnarBatch]] {
//          override def hasNext: Boolean = batches.hasNext
//
//          override def next(): Iterator[ColumnarBatch] = {
//            val batch = batches.next()
//            if (batch.numRows == 0) {
//              Iterator.empty
//            } else {
//              val start = System.currentTimeMillis()
//              val proj = MutableProjection.create(mutableProjectList, child.output)
//              val numRows = batch.numRows()
//              val c2rStart = System.currentTimeMillis()
//              val arrowBatch =
//                ColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance(), batch)
//              c2rTime += System.currentTimeMillis() - c2rStart
//              val selectedBatch = ColumnarBatches.select(batch, projectIndexInChild.toArray)
//
//              val schema =
//                SparkShimLoader.getSparkShims.structFromAttributes(mutableProjectOutput)
//              val vectors: Array[WritableColumnVector] = ArrowWritableColumnVector
//                .allocateColumns(numRows, schema)
//                .map {
//                  vector =>
//                    vector.setValueCount(numRows)
//                    vector.asInstanceOf[WritableColumnVector]
//                }
//              val targetRow = new MutableColumnarRow(vectors)
//              for (i <- 0 until numRows) {
//                targetRow.rowId = i
//                proj.target(targetRow).apply(arrowBatch.getRow(i))
//              }
//              val r2cStart = System.currentTimeMillis()
//              val targetBatch =
//                new ColumnarBatch(vectors.map(_.asInstanceOf[ColumnVector]), numRows)
//              val veloxBatch = ColumnarBatches
//                .ensureOffloaded(ArrowBufferAllocators.contextInstance(), targetBatch)
//              r2cTime += System.currentTimeMillis() - r2cStart
//              val composeBatch = if (selectedBatch.numCols() == 0) {
//                ColumnarBatches.retain(veloxBatch)
//                veloxBatch
//              } else {
//                val composeBatch = ColumnarBatches.composeWithReorder(selectedBatch, projectIndexes.toArray, veloxBatch)
//                veloxBatch.close()
//                composeBatch
//              }
//              timeMetric += System.currentTimeMillis() - start
//              arrowBatch.close()
//              targetBatch.close()
//              selectedBatch.close()
//              Iterator.single(composeBatch)
//            }
//          }
//        }
//        Iterators
//          .wrap(res.flatten)
//          .protectInvocationFlow() // Spark may call `hasNext()` again after a false output which
//          // is not allowed by Gluten iterators. E.g. GroupedIterator#fetchNextGroupIterator
//          .recyclePayload(_.close())
//          .create()
//    }
//  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val timeMetric = longMetric("time")
    val c2rTime = longMetric("column_to_row_time")
    val r2cTime = longMetric("row_to_column_time")
    val childOutput = child.output
    child.executeColumnar().mapPartitions {
      batches =>
        val res: Iterator[Iterator[ColumnarBatch]] = new Iterator[Iterator[ColumnarBatch]] {
          override def hasNext: Boolean = batches.hasNext

          override def next(): Iterator[ColumnarBatch] = {
            val batch = batches.next()
            if (batch.numRows == 0) {
              Iterator.empty
            } else {
              val start = System.currentTimeMillis()
              val proj = UnsafeProjection.create(mutableProjectList, childOutput)
              val c2rStart = System.currentTimeMillis()
              val rows = VeloxColumnarToRowExec.toRowIterator(Iterator.single(batch), childOutput).map(proj)
              c2rTime += System.currentTimeMillis() - c2rStart
              val selectedBatch = ColumnarBatches.select(batch, projectIndexInChild.toArray)
              val schema =
                SparkShimLoader.getSparkShims.structFromAttributes(mutableProjectOutput)
              val iter = RowToVeloxColumnarExec.toColumnarBatchIterator(
                rows,
                schema,
                batch.numRows()).map {
                b => if (selectedBatch.numCols() == 0) {
                  ColumnarBatches.retain(b)
                  selectedBatch.close()
                  b
                } else {
                  print("project batch" + ColumnarBatches.toString(b, 0, 20))
                  val composeBatch = ColumnarBatches.composeWithReorder(selectedBatch, projectIndexes.toArray, b)
                  b.close()
                  selectedBatch.close()
                  composeBatch
                }
              }
              timeMetric += System.currentTimeMillis() - start
              iter
            }
          }
        }
        Iterators
          .wrap(res.flatten)
          .protectInvocationFlow() // Spark may call `hasNext()` again after a false output which
          // is not allowed by Gluten iterators. E.g. GroupedIterator#fetchNextGroupIterator
          .recyclePayload(_.close())
          .create()
    }
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("MutableProject", mutableProjectList)}
       |""".stripMargin
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!GlutenConfig.getConf.enableProjectColumnarExec) {
      return ValidationResult.failed("Config disable this feature")
    }
    if (mutableProjectOutput.exists(f => !validateDataType(f.dataType))) {
      return ValidationResult.failed("Output type is not mutable")
    }
    if (mutableProjectOutput.size == projectList.size) {
      return ValidationResult.failed("All the project list is needed")
    }
    ValidationResult.succeeded
  }
}
