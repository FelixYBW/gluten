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
package org.apache.gluten.vectorized;

import org.apache.gluten.columnarbatch.ColumnarBatches;
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;
import org.apache.gluten.test.VeloxBackendTestBase;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.utils.SparkArrowUtil;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.task.TaskResources$;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import scala.collection.JavaConverters;

public class ArrowColumnVectorTest extends VeloxBackendTestBase {

  @Test
  public void testRefCount() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final ColumnarBatch batch = newBatch("a int", new GenericInternalRow(new Object[] {42}));
          final RefCountedArrowColumnVector col = (RefCountedArrowColumnVector) batch.column(0);
          Assert.assertEquals(1, col.refCnt());
          col.retain();
          Assert.assertEquals(2, col.refCnt());
          batch.close();
          Assert.assertEquals(1, col.refCnt());
          Assert.assertEquals(42, col.getInt(0));
          batch.close();
          Assert.assertEquals(0, col.refCnt());
          // Closing again is a no-op.
          batch.close();
          Assert.assertEquals(0, col.refCnt());
          return null;
        });
  }

  @Test
  public void testWriteDecimal() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final Decimal decimal = new Decimal();
          decimal.set(234, 20, 1);
          final ColumnarBatch batch =
              newBatch("a decimal(20, 1)", new GenericInternalRow(new Object[] {decimal}));
          Assert.assertEquals(decimal, batch.column(0).getDecimal(0, 20, 1));
          batch.close();
          return null;
        });
  }

  @Test
  public void testOffloadSparkArrowBatchFromAnotherAllocator() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 10;
          try (BufferAllocator foreign = new RootAllocator(Long.MAX_VALUE);
              IntVector vector = new IntVector("a", foreign)) {
            vector.allocateNew(numRows);
            for (int i = 0; i < numRows; i++) {
              vector.set(i, i * 3);
            }
            vector.setValueCount(numRows);
            // A batch of plain Spark ArrowColumnVectors, owned by its producer.
            final ColumnarBatch input =
                new ColumnarBatch(new ColumnVector[] {new ArrowColumnVector(vector)}, numRows);
            final ColumnarBatch offloaded =
                ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), input);
            Assert.assertTrue(ColumnarBatches.isLightBatch(offloaded));
            // The input batch is left to its producer.
            Assert.assertTrue(input.column(0) instanceof ArrowColumnVector);
            Assert.assertEquals(numRows, vector.getValueCount());
            Assert.assertEquals(9, input.column(0).getInt(3));
            final ColumnarBatch loaded =
                ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), offloaded);
            for (int i = 0; i < numRows; i++) {
              Assert.assertEquals(i * 3, loaded.column(0).getInt(i));
            }
            loaded.close();
          }
          return null;
        });
  }

  @Test
  public void testOffloadSparkArrowBatchFromSparkRootAllocator() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 10;
          // Like the vectors of Spark's Arrow Python runners, e.g. the results of Arrow UDFs.
          final BufferAllocator spark =
              SparkArrowUtil.rootAllocator().newChildAllocator("spark", 0, Long.MAX_VALUE);
          final IntVector vector = new IntVector("a", spark);
          vector.allocateNew(numRows);
          for (int i = 0; i < numRows; i++) {
            vector.set(i, i * 3);
          }
          vector.setValueCount(numRows);
          final ColumnarBatch input =
              new ColumnarBatch(new ColumnVector[] {new ArrowColumnVector(vector)}, numRows);
          final ColumnarBatch offloaded =
              ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), input);
          Assert.assertTrue(ColumnarBatches.isLightBatch(offloaded));
          // The buffers were moved, not copied: the producer's vector is left empty, and its
          // allocator can be closed while Gluten still uses them.
          Assert.assertEquals(0, vector.getValueCount());
          Assert.assertEquals(0, spark.getAllocatedMemory());
          vector.close();
          spark.close();
          final ColumnarBatch loaded =
              ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), offloaded);
          for (int i = 0; i < numRows; i++) {
            Assert.assertEquals(i * 3, loaded.column(0).getInt(i));
          }
          loaded.close();
          Assert.assertEquals(0, ArrowBufferAllocators.contextInstance().getAllocatedMemory());
          return null;
        });
  }

  /** Counts the bytes an allocation listener considers allocated. */
  private static final class CountingListener implements AllocationListener {
    private long allocated = 0L;

    @Override
    public void onPreAllocation(long size) {
      allocated += size;
    }

    @Override
    public void onRelease(long size) {
      allocated -= size;
    }
  }

  @Test
  public void testAdoptMovesAllocationListenerAccounting() {
    final CountingListener sourceListener = new CountingListener();
    final CountingListener targetListener = new CountingListener();
    try (BufferAllocator source =
            SparkArrowUtil.rootAllocator()
                .newChildAllocator("source", sourceListener, 0, Long.MAX_VALUE);
        BufferAllocator target =
            SparkArrowUtil.rootAllocator()
                .newChildAllocator("target", targetListener, 0, Long.MAX_VALUE)) {
      final IntVector vector = new IntVector("a", source);
      vector.allocateNew(10);
      vector.setValueCount(10);
      final long size = sourceListener.allocated;
      Assert.assertTrue(size > 0);
      final FieldVector adopted = ArrowColumnVectors.adopt(vector, target);
      // Moved without copying, with its accounting: allocation listeners are not notified of
      // transfers, but the target's one is when the buffers are released.
      Assert.assertEquals(0L, source.getAllocatedMemory());
      Assert.assertEquals(0L, sourceListener.allocated);
      Assert.assertEquals(size, targetListener.allocated);
      vector.close();
      adopted.close();
      Assert.assertEquals(0L, targetListener.allocated);
    }
  }

  private static ColumnarBatch newBatch(String schema, InternalRow row) {
    return ArrowColumnVectors.fromRows(
        StructType.fromDDL(schema),
        JavaConverters.asScalaIterator(Collections.singletonList(row).iterator()),
        ArrowBufferAllocators.contextInstance());
  }
}
