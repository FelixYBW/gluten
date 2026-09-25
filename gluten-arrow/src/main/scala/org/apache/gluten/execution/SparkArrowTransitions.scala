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

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.{ArrowJavaBatchType, SparkArrowBatchType}
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, ValueVector}
import org.apache.arrow.vector.util.VectorAppender

import scala.collection.JavaConverters._

/**
 * Converts [[ArrowJavaBatchType]] to [[SparkArrowBatchType]] without copying: the Arrow vectors are
 * transferred into Spark [[ArrowColumnVector]]s owned by the output batch, which is released when
 * the next batch is requested.
 */
case class ArrowJavaToSparkArrowExec(override val child: SparkPlan)
  extends ColumnarToColumnarExec(child)
  with GlutenColumnarToColumnarTransition {

  override protected val from: Convention.BatchType = ArrowJavaBatchType

  override protected val to: Convention.BatchType = SparkArrowBatchType

  override protected def needRecyclePayload: Boolean = true

  override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    in.map {
      b =>
        val columns = (0 until b.numCols).map {
          i =>
            val vector = b.column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector
            new ArrowColumnVector(SparkArrowTransitions.transfer(vector, vector.getAllocator))
              .asInstanceOf[ColumnVector]
        }
        new ColumnarBatch(columns.toArray, b.numRows)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

/**
 * Converts [[SparkArrowBatchType]] to [[ArrowJavaBatchType]]. Vectors allocated by Gluten's
 * allocator are transferred without copying. Vectors from other allocators (e.g. the Python
 * worker's output, read by Spark into an allocator it closes once the stream ends) are copied into
 * Gluten's allocator, since Arrow buffers cannot be shared across allocator roots.
 */
case class SparkArrowToArrowJavaExec(override val child: SparkPlan)
  extends ColumnarToColumnarExec(child)
  with GlutenColumnarToColumnarTransition {

  override protected val from: Convention.BatchType = SparkArrowBatchType

  override protected val to: Convention.BatchType = ArrowJavaBatchType

  override protected def needRecyclePayload: Boolean = true

  override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    in.map {
      b =>
        val allocator = ArrowBufferAllocators.contextInstance()
        val vectors = (0 until b.numCols).map {
          i =>
            val vector = b.column(i).asInstanceOf[ArrowColumnVector].getValueVector
            if (vector.getAllocator.getRoot == allocator.getRoot) {
              SparkArrowTransitions.transfer(vector, allocator)
            } else {
              SparkArrowTransitions.copy(vector, allocator)
            }
        }
        new ColumnarBatch(
          ArrowWritableColumnVector
            .loadColumns(b.numRows, vectors.asJava)
            .map(_.asInstanceOf[ColumnVector]),
          b.numRows)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

object SparkArrowTransitions {

  /** Moves the buffers of `vector` into a new vector, leaving `vector` empty. */
  def transfer(vector: ValueVector, allocator: BufferAllocator): FieldVector = {
    val pair = vector.getTransferPair(allocator)
    pair.transfer()
    pair.getTo.asInstanceOf[FieldVector]
  }

  /** Copies `vector` into a new vector allocated by `allocator`. */
  def copy(vector: ValueVector, allocator: BufferAllocator): FieldVector = {
    val target = vector.getField.createVector(allocator)
    target.allocateNew()
    vector.accept(new VectorAppender(target), null)
    target
  }
}
