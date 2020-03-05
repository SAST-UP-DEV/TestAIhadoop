/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

/**
 * OutputStream Statistics Implementation for Abfs.
 */
public class AbfsOutputStreamStatisticsImpl
    implements AbfsOutputStreamStatistics {
  private long bytesToUpload;
  private long bytesUploadSuccessful;
  private long bytesUploadFailed;
  /**
   * counter to get the total time spent while waiting for tasks to complete
   * in the Blocking queue inside the thread executor.
   */
  private long timeSpendOnTaskWait;
  /**
   * counter to get the total number of queue shrink operations done{@code
   * AbfsOutputStream#shrinkWriteOperationQueue()} by
   * AbfsOutputStream to remove the write operations which were successfully
   * done by AbfsOutputStream from the Blocking Queue.
   */
  private long queueShrunkOps;
  /**
   * counter to get the total number of times the current buffer is written
   * to the service{@code AbfsOutputStream#writeCurrentBufferToService()} via
   * AbfsClient and appended to the
   * Data store by
   * AbfsResOperation.
   */
  private long writeCurrentBufferOperations;

  /**
   * Records the need to upload bytes and increments the total bytes that
   * needs to be uploaded.
   *
   * @param bytes Total bytes to upload. Negative bytes are ignored.
   */
  @Override
  public void bytesToUpload(long bytes) {
    if (bytes > 0) {
      bytesToUpload += bytes;
    }
  }

  /**
   * Records the total bytes successfully uploaded through AbfsOutputStream.
   *
   * @param bytes number of bytes that were successfully uploaded. Negative
   *              bytes are ignored.
   */
  @Override
  public void uploadSuccessful(long bytes) {
    if (bytes > 0) {
      bytesUploadSuccessful += bytes;
    }
  }

  /**
   * Records the total bytes failed to upload through AbfsOutputStream.
   *
   * @param bytes number of bytes failed to upload. Negative bytes are ignored.
   */
  @Override
  public void uploadFailed(long bytes) {
    if (bytes > 0) {
      bytesUploadFailed += bytes;
    }
  }

  /**
   * {@inheritDoc}
   *
   * Records the total time spent waiting for a task.
   * When the thread executor has a task
   * queue{@link java.util.concurrent.BlockingQueue} of size greater than or equal to 2
   * times the maxConcurrentRequestCounts then, it waits for a task in that
   * queue to finish, then do the next task in the queue.
   *
   * This time spent while waiting for the task to be completed is being
   * recorded in this counter.
   *
   * @param startTime time(in milliseconds) before the wait for task to be
   *                  completed is begin.
   * @param endTime   time(in milliseconds) after the wait for the task to be
   *                  completed is done.
   */
  @Override
  public void timeSpentTaskWait(long startTime, long endTime) {
    timeSpendOnTaskWait += endTime - startTime;
  }

  /**
   * {@inheritDoc}
   *
   * Records the number of times AbfsOutputStream try to remove the completed
   * write operations from the beginning of write operation FIFO queue.
   */
  @Override
  public void queueShrunk() {
    queueShrunkOps++;
  }

  /**
   * {@inheritDoc}
   *
   * Records the number of times AbfsOutputStream writes the buffer to the
   * service via the AbfsClient and appends the buffer to the service.
   */
  @Override
  public void writeCurrentBuffer() {
    writeCurrentBufferOperations++;
  }

  public long getBytesToUpload() {
    return bytesToUpload;
  }

  public long getBytesUploadSuccessful() {
    return bytesUploadSuccessful;
  }

  public long getBytesUploadFailed() {
    return bytesUploadFailed;
  }

  public long getTimeSpendOnTaskWait() {
    return timeSpendOnTaskWait;
  }

  public long getQueueShrunkOps() {
    return queueShrunkOps;
  }

  public long getWriteCurrentBufferOperations() {
    return writeCurrentBufferOperations;
  }

  /**
   * String to show AbfsOutputStream statistics values in AbfsOutputStream.
   *
   * @return String with AbfsOutputStream statistics.
   */
  @Override public String toString() {
    final StringBuilder outputStreamStats = new StringBuilder(
        "OutputStream Statistics{");
    outputStreamStats.append(", BYTES_UPLOAD=").append(bytesToUpload);
    outputStreamStats.append(", BYTES_UPLOAD_SUCCESSFUL=")
        .append(bytesUploadSuccessful);
    outputStreamStats.append(", BYTES_UPLOAD_FAILED=")
        .append(bytesUploadFailed);
    outputStreamStats.append(", TIME_SPEND_WAIT_TASK=")
        .append(timeSpendOnTaskWait);
    outputStreamStats.append(", QUEUE_SHRINK=").append(queueShrunkOps);
    outputStreamStats.append(", WRITE_CURRENT_BUFFER=")
        .append(writeCurrentBufferOperations);
    outputStreamStats.append("}");
    return outputStreamStats.toString();
  }
}
