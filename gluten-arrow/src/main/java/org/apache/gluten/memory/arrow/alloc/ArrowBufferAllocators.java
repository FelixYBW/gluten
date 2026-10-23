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
package org.apache.gluten.memory.arrow.alloc;

import org.apache.gluten.config.GlutenConfig;
import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.memory.memtarget.MemoryTargets;
import org.apache.gluten.memory.memtarget.Spillers;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.memory.GlobalOffHeapMemory;
import org.apache.spark.task.TaskResource;
import org.apache.spark.task.TaskResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Vector;

public class ArrowBufferAllocators {
  private static final SimpleMemoryUsageRecorder GLOBAL_USAGE = new SimpleMemoryUsageRecorder();
  private static final BufferAllocator GLOBAL_INSTANCE;

  static {
    final AllocationListener listener =
        new ManagedAllocationListener(GlobalOffHeapMemory.target(), GLOBAL_USAGE);
    GLOBAL_INSTANCE = new RootAllocator(listener, Long.MAX_VALUE);
  }

  private ArrowBufferAllocators() {}

  // FIXME: Remove this then use contextInstance(name) instead
  public static BufferAllocator contextInstance() {
    return contextInstance("Default");
  }

  public static BufferAllocator contextInstance(String name) {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method must be called in a Spark task.");
    }
    String id = "ArrowBufferAllocatorManager:" + name;
    return TaskResources.addResourceIfNotRegistered(id, () -> new ArrowBufferAllocatorManager(name))
        .managed;
  }

  /**
   * The untracked child of {@link #contextInstance()}, see {@link
   * #contextUntrackedInstance(String)}.
   */
  public static BufferAllocator contextUntrackedInstance() {
    return contextUntrackedInstance("Default");
  }

  /**
   * A child of {@link #contextInstance(String)} that doesn't reserve Spark memory. Use it to import
   * Arrow buffers whose memory is already reserved by their owner, e.g. buffers exported from a
   * native batch, which stay allocated from the native memory pool until they are released. Being
   * in the same allocator tree, the imported buffers can still be moved to or shared with vectors
   * of {@link #contextInstance(String)}.
   */
  public static BufferAllocator contextUntrackedInstance(String name) {
    final BufferAllocator parent = contextInstance(name);
    String id = "ArrowBufferAllocatorManager:Untracked:" + name;
    return TaskResources.addResourceIfNotRegistered(
            id, () -> new UntrackedArrowBufferAllocatorManager(name, parent))
        .managed;
  }

  public static BufferAllocator globalInstance() {
    return GLOBAL_INSTANCE;
  }

  public static class ArrowBufferAllocatorManager implements TaskResource {
    private static Logger LOGGER = LoggerFactory.getLogger(ArrowBufferAllocatorManager.class);
    private static final List<BufferAllocator> LEAKED = new Vector<>();
    private final AllocationListener listener;
    private final String name;

    {
      if (GlutenConfig.get().memoryUntracked()) {
        listener = AllocationListener.NOOP;
      } else {
        listener =
            new ManagedAllocationListener(
                MemoryTargets.throwOnOom(
                    MemoryTargets.dynamicOffHeapSizingIfEnabled(
                        MemoryTargets.newConsumer(
                            "ArrowContextInstance", Spillers.NOOP, Collections.emptyMap()))),
                TaskResources.getSharedUsage());
      }
    }

    private final BufferAllocator managed = new RootAllocator(listener, Long.MAX_VALUE);

    public ArrowBufferAllocatorManager(String name) {
      this.name = name;
    }

    private void close() {
      managed.close();
    }

    private void softClose() {
      // move to leaked list
      long leakBytes = managed.getAllocatedMemory();
      long accumulated = TaskResources.ACCUMULATED_LEAK_BYTES().addAndGet(leakBytes);
      LOGGER.warn(
          String.format(
              "Detected leaked Arrow allocator [%s], size: %d, "
                  + "process accumulated leaked size: %d...",
              resourceName(), leakBytes, accumulated));
      if (TaskResources.DEBUG()) {
        LOGGER.warn(String.format("Leaked allocator stack %s", managed.toVerboseString()));
        LEAKED.add(managed);
      }
    }

    @Override
    public void release() throws Exception {
      if (managed.getAllocatedMemory() != 0L) {
        softClose();
      } else {
        close();
      }
    }

    @Override
    public int priority() {
      return 10; // low priority: released after higher-priority task resources
    }

    @Override
    public String resourceName() {
      return name;
    }
  }

  private static class UntrackedArrowBufferAllocatorManager implements TaskResource {
    private static final Logger LOGGER =
        LoggerFactory.getLogger(UntrackedArrowBufferAllocatorManager.class);
    private final String name;
    private final BufferAllocator managed;

    private UntrackedArrowBufferAllocatorManager(String name, BufferAllocator parent) {
      this.name = "Untracked:" + name;
      // Only the allocator that creates a buffer reports it to its listener, so the parent's
      // listener doesn't reserve Spark memory for the buffers of this child.
      this.managed =
          parent.newChildAllocator(this.name, AllocationListener.NOOP, 0, Long.MAX_VALUE);
    }

    @Override
    public void release() throws Exception {
      final long leakBytes = managed.getAllocatedMemory();
      if (leakBytes != 0L) {
        // Leave it open: the parent allocator then reports the leak too.
        LOGGER.warn(
            String.format("Detected leaked Arrow allocator [%s], size: %d", name, leakBytes));
        return;
      }
      managed.close();
    }

    @Override
    public int priority() {
      return 11; // released before the parent allocator, which can only be closed after it
    }

    @Override
    public String resourceName() {
      return name;
    }
  }
}
